var kafka = require('kafka-node')
 
const client = new kafka.Client("localhost:2181");
 
const topics = [
    {
        topic: "webevents.dev",
        offset: 0,
    }
];
const options = {
    fromOffset: true,
    autoCommit: true,
    fetchMaxWaitMs: 1024,
    fetchMaxBytes: 512,
    encoding: "buffer"
};
 
const consumer = new kafka.HighLevelConsumer(client, topics, options);
var lastOffset = 0;

consumer.on("message", function(message) {
    //console.log("consumer get an message", message, message.offset)
    // console.log("consumer offset", message.offset)
    if (message.offset > lastOffset) {
        lastOffset = message.offset 
    }
    // console.log("message.offset > lastOffset ", message.offset, lastOffset, message.offset > lastOffset)
    // Read string into a buffer.
    var buf = new Buffer(message.value, "binary"); 
    var decodedMessage = JSON.parse(buf.toString());
    
    let json = {
        id: decodedMessage.id,
        type: decodedMessage.type,
        userId: decodedMessage.userId,
        sessionId: decodedMessage.sessionId,
        data: JSON.stringify(decodedMessage.data),
        createdAt: new Date()
    }
    console.log("json ", json.data)
    //Events is a Sequelize Model Object. 
    // return Events.create(json);

    // only get 1 message
    consumer.pause()

});
 
consumer.on("error", function(err) {
    console.log("consumer error", err);
});
 
process.on("SIGINT", function() {
    consumer.close(true, function() {
        process.exit();
    });
});

function turnOnconsumer() {
    console.log("--------------------------")
    consumer.topicPayloads[0].offset = lastOffset +1 
    // console.log("changedpayload", consumer.topicPayloads)
    consumer.resume()
}

setInterval(turnOnconsumer, 2000)