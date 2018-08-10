var KafkaService = require("./kafkaservice");
//var consumer = require("./kafkaconsumer");


function sendTest(i ){
    let type = "type" + i
    let userId = "userId" + i
    let sessionId = "sessionId" + i
    let data = "data " + i
    KafkaService.sendRecord(data, function(){
        console.log("sended", i)
    })
}

function sendLoop(i){
    sendTest(i)
    setTimeout(function(){sendLoop(i+1)}, 10)
}

sendLoop(0)
