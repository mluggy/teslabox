const log = require('../log')
const config = require('../config')
const ping = require('../ping')
const s3 = require('../aws/s3')
const telegram = require('../telegram')
const controllers = require('../http/controllers')

const _ = require('lodash')
const async = require('async')
const glob = require('glob')
const path = require('path')
const fs = require('fs')
const { exec } = require('child_process')

const interval = 10000
const parallel = 1
const expires = 7 * 86400

const ramDir = process.env.NODE_ENV === 'production' ? '/mnt/ram' : path.join(__dirname, '../../mnt/ram')
const bucket = process.env.AWS_S3_BUCKET

const archives = []

exports.start = (cb) => {
  cb = cb || function () {}

  async.forever((next) => {
    const isArchive = config.get('archive')
    if (!isArchive) {
      return setTimeout(next, interval)
    }

    glob(`${ramDir}/archive/*/event.json`, (err, result) => {
      if (err) {
        return setTimeout(next, interval)
      }

      const carName = config.get('carName')
      const archiveQuality = config.get('archiveQuality').toUpperCase()
      const archiveCompression = config.get('archiveCompression')
      const archiveSeconds = Number(config.get('archiveSeconds'))
      const telegramRecipients = _.split(config.get('telegramRecipients'), ',')

      async.eachLimit(result, parallel, (row, cb) => {
        const parts = row.split(/[\\/]/)
        const folder = _.nth(parts, -2)

        let event
        let noAudioOutputFile
        let outputFile

        async.series([
          (cb) => {
            fs.readFile(row, (err, result) => {
              if (!err) {
                try {
                  event = JSON.parse(result)
                  if (event.type === 'sentry') {
                    event.angle = ['3', '5'].includes(event.camera) ? 'left' : ['4', '6'].includes(event.camera) ? 'right' : event.camera === '7' ? 'back' : 'front'
                  } else {
                    event.type = 'dashcam'
                  }
                } catch (e) {
                  err = e
                }
              }

              cb(err)
            })
          },
          (cb) => {
            noAudioOutputFile = row.replace('event.json', `${event.type}-no-audio.mp4`)

            fs.stat(noAudioOutputFile, (err, result) => {
              if (err || !result) {
                exec(`tesla_dashcam --no-check_for_update --no-notification --exclude_subdirs --temp_dir ${ramDir}/temp ${event.camera === 'rear' ? '--swap_frontrear ' : ''} --layout WIDESCREEN --quality ${archiveQuality} --compression ${archiveCompression} --sentry_start_offset=-${Math.ceil(archiveSeconds / 2)} --sentry_end_offset=${archiveSeconds - Math.ceil(archiveSeconds / 2)} --start_offset=-${archiveSeconds} ${row.replace('event.json', '')} --timestamp_format="TeslaBox ${_.upperFirst(event.type)} %Y-%m-%d %X" --output ${noAudioOutputFile}`, cb)
              } else {
                cb()
              }
            })
          },
          (cb) => {
            outputFile = row.replace('event.json', `${event.type}.mp4`)

            fs.stat(outputFile, (err, result) => {
              if (err || !result) {
                exec(`ffmpeg -f lavfi -i anullsrc=channel_layout=stereo:sample_rate=44100 -i ${noAudioOutputFile} -c:v copy -c:a aac -shortest ${outputFile}`, cb)
              } else {
                cb()
              }
            })
          },
          (cb) => {
            if (!ping.isAlive()) {
              return cb()
            }

            fs.readFile(outputFile, (err, result) => {
              if (err) {
                return cb(err)
              }

              const key = `${process.env.AWS_ACCESS_KEY_ID}/archive/${folder}/${event.type}.mp4`
              let url

              async.series([
                (cb) => {
                  s3.putObject(bucket, key.replace(`${event.type}.mp4`, 'event.json'), JSON.stringify(event), cb)
                },
                (cb) => {
                  s3.putObject(bucket, key, result, cb)
                },
                (cb) => {
                  s3.getSignedUrl(bucket, key, expires, (err, result) => {
                    if (!err && result) {
                      url = result
                    }

                    cb(err)
                  })
                },
                (cb) => {
                  const message = `${carName} ${_.upperFirst(event.type)} ${controllers.formatDate(event.adjustedTimestamp)}\n${url ? `[Download](${url}) | ` : ''}[Map](https://www.google.com/maps?q=${event.est_lat},${event.est_lon})`

                  async.parallel([
                    (cb) => {
                      telegram.sendVideo(telegramRecipients, url, message, true, (err) => {
                        if (err) {
                          log.warn(`archive telegram failed: ${err}`)
                        }

                        cb()
                      })
                    }
                  ], cb)
                }
              ], (err) => {
                if (err) {
                  log.warn(`archive failed: ${err}`)
                } else {
                  archives.push({
                    created: new Date(event.adjustedTimestamp),
                    lat: event.est_lat,
                    lon: event.est_lon,
                    type: event.type,
                    folder,
                    url,
                    processed: new Date()
                  })

                  log.debug(`archived ${folder}`)
                }

                cb()
              })
            })
          }
        ], (err) => {
          if (err || _.find(archives, { folder })) {
            fs.rmdir(`${ramDir}/archive/${folder}`, { recursive: true }, (err) => {
              if (err) {
                log.warn(`archiving delete failed: ${err}`)
              }

              cb()
            })
          } else {
            cb()
          }
        })
      }, () => {
        setTimeout(next, interval)
      })
    })
  })

  cb()
}

exports.list = () => {
  return archives
}
