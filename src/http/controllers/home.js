const config = require('../../config')
const controllers = require('./')
const package = require('../../../package.json')

const _ = require('lodash')

const isPublicPassword = !!process.env.PUBLIC_PASSWORD

const split = (str) => {
  return _.join(_.split(str, /[\r\n, ]+/), ',')
}

module.exports = (req, res, next) => {
  if (req.method === 'POST') {
    config.set('carName', req.body.carName || '')
    config.set('logLevel', req.body.logLevel || 'debug')
    config.set('archive', req.body.archive === 'on')
    config.set('archiveSeconds', req.body.archiveSeconds || 30)
    config.set('archiveQuality', req.body.archiveQuality || 'lowest')
    config.set('archiveCompression', req.body.archiveCompression || 'superfast')
    config.set('telegramRecipients', split(req.body.telegramRecipients))
    config.set('stream', req.body.stream === 'on')
    config.set('ssh', req.body.ssh === 'on')
    config.set('public', req.body.public === 'on')

    res.location('/')
    return next()
  }

  const archiveQuality = config.get('archiveQuality')
  const archiveCompression = config.get('archiveCompression')
  const logLevel = config.get('logLevel')

  const locals = {
    carName: config.get('carName'),
    logLevelDebug: logLevel === 'debug',
    logLevelInfo: logLevel === 'info',
    logLevelWarning: logLevel === 'warning',
    logLevelError: logLevel === 'error',
    archive: !!config.get('archive'),
    archiveSeconds: config.get('archiveSeconds'),
    archiveQualityLowest: archiveQuality === 'lowest',
    archiveQualityLower: archiveQuality === 'lower',
    archiveQualityLow: archiveQuality === 'low',
    archiveQualityMedium: archiveQuality === 'medium',
    archiveQualityHigh: archiveQuality === 'high',
    archiveCompressionUltrafast: archiveCompression === 'ultrafast',
    archiveCompressionSuperfast: archiveCompression === 'superfast',
    archiveCompressionVeryfast: archiveCompression === 'veryfast',
    archiveCompressionFaster: archiveCompression === 'faster',
    archiveCompressionFast: archiveCompression === 'fast',
    archiveCompressionMedium: archiveCompression === 'medium',
    archiveCompressionSlow: archiveCompression === 'slow',
    archiveCompressionSlower: archiveCompression === 'slower',
    archiveCompressionVeryslow: archiveCompression === 'veryslow',
    telegramRecipients: config.get('telegramRecipients'),
    stream: !!config.get('stream'),
    ssh: !!config.get('ssh'),
    public: !!config.get('public'),
    isPublicPassword,
    time: controllers.formatDate(),
    userIp: req.ip,
    userAgent: req.get('User-Agent'),
    version: package.version
  }

  res.render('home', locals, (err, result) => {
    if (!err) {
      res.locals.response = result
    }

    next(err)
  })
}
