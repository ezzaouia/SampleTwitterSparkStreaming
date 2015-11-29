'use strict'

const
  express = require('express'),
  app = require('express')(),
  server = require('http').Server(app),
  io = require('socket.io')(server),
  MongoClient = require('mongodb').MongoClient,
  assert = require('assert'),
  DB_URL = 'mongodb://localhost:27017/appTwitterStream'

server.listen(3000)

// serving /www dirname as static files to client
app.use(express.static(__dirname + '/www'))


MongoClient.connect(DB_URL, function (err, db) {
  assert.equal(null, err);
  db.collection("tweets", function (err, collection) {
    collection.isCapped(function (err, capped) {
      if (err) {
        assert.ifError(err)
      }
      if (!capped) {
        console.log(collection.collectionName + " is not a capped collection for tailable cursor")
        process.exit(2);
      }
      console.log("Connected correctly to server.")

      // start the server
      serverLift(collection)
    })
  })
})

// start server 
function serverLift(collection) {
  console.log("Starting .. socket on the top of express")
  io.sockets.on("connection", function (socket) {
    console.log("client connected")
    sendDataStream(socket, collection)
  })
}

// read and send data stream to client
function sendDataStream(socket, collection) {
  let dataStream = collection.find({}, {
    tailable: true,
    awaitdata: true,
    numberOfRetries: Number.MAX_VALUE,
    tailableRetryInterval: 1000,
    timeout: false
  }).stream()

  dataStream.on('data', function (data) {
    console.log(data)
    socket.emit("tweets", data)
  })

  dataStream.on('error', function (error) {
    console.log(error)
  })

  dataStream.on('end', function () {
    console.log('End of stream')
  })
}