var layouts = require('../layouts')

var cloudWatchConfig = {}
var cloudWatchLogs

var sequenceToken
var streamReady = false

var layout
var config

var messages = []

function cloudWatchAppender(_config, _layout) {

  config = _config
  layout = _layout || layouts.basicLayout

  return function (loggingEvent) {
    if(messages.length > 100)
      messages = messages.slice(1)
    messages.push({
      message: layout(loggingEvent, config.timezoneOffset),
      timestamp: Date.now()
    })
    maybeFlushStream()
  }
}

function configure(_config) {
  config = _config
  cloudWatchConfig = _config.cloudWatchConfig || {}
  const AWS = cloudWatchConfig.AWS
  cloudWatchLogs = AWS ? new AWS.CloudWatchLogs() : undefined

  if (_config.layout)
    layout = layouts.layout(_config.layout.type, _config.layout)

  initStream()

  return cloudWatchAppender(_config, layout)
}

exports.appender = cloudWatchAppender
exports.configure = configure

var initInProgress = false
var START_BACKOFF = 10 * 1000
var backoff = START_BACKOFF

function initStream() {
  if(initInProgress || !cloudWatchLogs) return
  initInProgress = true
  var params = {
    logGroupName: cloudWatchConfig.logGroupName
    //logStreamNamePrefix: cloudWatchConfig.logStreamName,
    //nextToken: 'STRING_VALUE',
    //orderBy: 'LogStreamName'
  }
  cloudWatchLogs.describeLogStreams(params, function(err, data) {
    initInProgress = false
    if(err) {
      console.error('could not describe streams', err.stack || err)
    } else {
      var stream
      if(Array.isArray(data.logStreams)) {
        data.logStreams.forEach(function(logStream) {
          if(logStream.logStreamName === cloudWatchConfig.logStreamName) {
            stream = logStream
          }
        })
      }
      if(stream) {
        sequenceToken = stream.uploadSequenceToken
        initStreamSuccess()
      } else {
        cloudWatchLogs.createLogStream({
          logGroupName: cloudWatchConfig.logGroupName,
          logStreamName: cloudWatchConfig.logStreamName
        }, function(err, data) {
          if(err) {
            console.error('could not create stream', err.stack || err)
          } else {
            sequenceToken = undefined
            initStreamSuccess()
          }
        })
      }
    }
  })
}

function initStreamSuccess() {
  streamReady = true
  backoff = START_BACKOFF
  maybeFlushStream()
}

var initScheduled = false

function scheduleInitStream() {
  if(!initScheduled) {
    initScheduled = true
    setTimeout(function() {
      initScheduled = false
      initStream()
    }, backoff)
    backoff *= 2
  }
}

var flushInProgress = false

function maybeFlushStream() {
  if(flushInProgress || !streamReady || !messages.length || !cloudWatchLogs) return
  flushInProgress = true
  var messagesToSend = messages
  messages = []
  var params = {
    logEvents: messagesToSend,
    logGroupName: cloudWatchConfig.logGroupName,
    logStreamName: cloudWatchConfig.logStreamName,
    sequenceToken: sequenceToken
  }
  cloudWatchLogs.putLogEvents(params, function(err, data) {
    flushInProgress = false
    if(err) {
      streamReady = false
      console.error('could not put event: ', err.stack || err)
      scheduleInitStream()
    } else {
      sequenceToken = data.nextSequenceToken
      if(messages.length) {
        // More messages came in while we were sending the last batch
        setTimeout(maybeFlushStream, 100)
      }
    }
  })
}
