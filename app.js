'use strict'

var mysql = require('mysql');
var AWS = require('aws-sdk')
var firehose = new AWS.Firehose({
    region: 'us-west-2'
});
const Gdax = require('gdax');
const websocket = new Gdax.WebsocketClient(['BTC-USD', 'ETH-USD']);

var query = "INSERT INTO `mysqldb1`.`orders` (`order_id`,`type`,`side`,`reason`,`product_id`,`price`,`remaining_size`,`sequence`,`time`) VALUES"

AWS.config.update({region: 'us-west-2'})

getSecret()

function connectMySQL(secret){
    console.log('connecting db')
    var con = mysql.createConnection({
        host: secret.host,
        user: secret.username,
        password: secret.password
    });
    
    con.connect(function (err) {
        if (err) throw err;
        console.log("DB Connected!");
    
        websocket.on('message', trade => {
            insertToMySQL(con, trade);
            putKinesis(trade)
        });
    });
}

function insertToMySQL(con, trade) {
    if (trade.price) {
        var price = parseFloat(trade.price)
        var remaining_size = 0;
        if (trade.remaining_size) remaining_size = parseFloat(trade.remaining_size)

        var sql = query + "('" + trade.order_id + "','" + trade.type + "','" + trade.side + "','" +
            trade.reason + "','" + trade.product_id + "'," + price + "," + remaining_size +
            "," + trade.sequence + ",'" + trade.time + "')"

        con.query(sql, function (err, result) {
            if (err) {
                console.log(err, trade, sql)
            } else {
                // console.log(result)
            }
        })
    }

}

function putKinesis(trade) {
    if (trade.price) {
        var price = parseFloat(trade.price)
        var remaining_size = 0;
        if (trade.remaining_size) remaining_size = parseFloat(trade.remaining_size)

        var data = trade.order_id + '\t' + trade.type + '\t' + trade.side +
            '\t' + trade.reason + '\t' + trade.product_id + '\t' + price + '\t' + remaining_size +
            '\t' + trade.sequence + '\t' + trade.time

        var params = {
            DeliveryStreamName: 'gdax',
            Record: {
                Data: data
            }
        }

        firehose.putRecord(params, function (err, response) {
            if (err) console.log(err, err.stack); // an error occurred
            // selse console.log(data); // successful response
        })
    }
}

function getSecret() {
    console.log('getting secrets')
    var endpoint = "https://secretsmanager.us-west-2.amazonaws.com",
        region = "us-west-2",
        secretName = "mysql-rds-access",
        secret,
        binarySecretData;
    // Create a Secrets Manager client
    var client = new AWS.SecretsManager({
        endpoint: endpoint,
        region: region
    });

    client.getSecretValue({SecretId: secretName}, function(err, data) {
        console.log('getSecretValue completed')
        if(err) {
            if(err.code === 'ResourceNotFoundException')
                console.log("The requested secret " + secretName + " was not found");
            else if(err.code === 'InvalidRequestException')
                console.log("The request was invalid due to: " + err.message);
            else if(err.code === 'InvalidParameterException')
                console.log("The request had invalid params: " + err.message);
            else {
                console.log(err)
            }
        }
        else {
            // Decrypted secret using the associated KMS CMK
            // Depending on whether the secret was a string or binary, one of these fields will be populated
            if(data.SecretString !== "") {
                secret = data.SecretString;
            } else {
                binarySecretData = data.SecretBinary;
            }
        }
        
        connectMySQL(JSON.parse(secret));
        
    });
}



websocket.on('error', err => {
    console.log(err)
    /* handle error */
});
websocket.on('close', () => {
    console.log('connection closed')
    /* ... */
});