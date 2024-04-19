import postgres from 'postgres';
import getPort from 'get-port';
import { Kafka } from 'kafkajs';
import { networkInterfaces } from 'os';

const args = process.argv;
const topic = 'topic_for_test';
const groupId = 'test-consumer-group';
const clientId = 'test-consumer-group';

async function getDefaultPort(){
    return await getPort();
}

function getLocalIp(){
    const nets = networkInterfaces();
    const results = Object.create(null); // Or just '{}', an empty object

    for (const name of Object.keys(nets)) {
        for (const net of nets[name]) {
            // Skip over non-IPv4 and internal (i.e. 127.0.0.1) addresses
            // 'IPv4' is in Node <= 17, from 18 it's a number 4 or 6
            const familyV4Value = typeof net.family === 'string' ? 'IPv4' : 4
            if (net.family === familyV4Value && !net.internal) {
                if (!results[name]) {
                    results[name] = [];
                }
                results[name].push(net.address);
            }
        }
    }
    console.log(`nets : ${JSON.stringify(nets)}`)
    console.log(`nets filtred : ${JSON.stringify(results)}`)
    return results["vEthernet (WSL (Hyper-V firewall))"][0];
}

async function testKafkaConnect(){
    let hostPostgres;
    let portPostgres;
    let broker;
    if(args[2]){
        broker = args[2];
    }else{
        const port = await getDefaultPort();
        const ip = getLocalIp();
        broker = `${ip}:${port}`;
    }
    if(args[3] && args[4]){
        hostPostgres = args[3];
        portPostgres = args[4];
    }else{
        hostPostgres = getLocalIp();
        portPostgres = await getDefaultPort();
    }
    console.log(`broker ${broker}`);
    console.log(`hostPostgres ${hostPostgres}`);
    console.log(`portPostgres ${portPostgres}`);

    const kafka = new Kafka({
        clientId,
        brokers: [broker],
    });

    const admin = kafka.admin()

    // remember to connect and disconnect when you are done
    await admin.connect()
    await admin.createTopics({
        topics: [{
            topic,
        }],
    })
    await admin.disconnect()

    const producer = kafka.producer()

    await producer.connect()
    await producer.send({
        topic,
        messages: [
            { value: 'Hello KafkaJS user!' },
        ],
    })

    await producer.disconnect()

    const consumer = kafka.consumer({ groupId })

    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
                console.log({
                value: message.value.toString(),
            })
        },
    })
}

async function testPostgreConnect(){
    const sql = postgres({
        host                 : args[3],            // Postgres ip address[s] or domain name[s]
        port                 : args[4],          // Postgres server port[s]
        database             : 'postgres',            // Name of database to connect to
        username             : 'postgres',            // Username of database user
        password             : 'Password1',            // Password of database user
      });
    console.log(sql);
    console.log('postgres connected!!');
}
async function test(){
    await testKafkaConnect();
    await testPostgreConnect();
}
test();
