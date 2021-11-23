const { Pool } = require('pg');
const MongoClient = require('mongodb').MongoClient;
const uri = 'mongodb://localhost:27017';
const assert = require('assert');

//global let for the name of used mongoDB database
let mongoDbName = 'PDT'

//Postgres connect
const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'PDT',
    password: 'postgres',
    port: 5433
});

pool.on('error', (err, client) => {
    console.error('Unexpected error on idle client', err);
    process.exit(-1);
});

//Mongo connect
const client = new MongoClient(uri, {
    useUnifiedTopology: true,
});

client.connect(function (err, db) {
    //assert.strictEqual(null, err);
    console.log('Connected to MongoDB');
    if (err)
        db.close();
});

async function loadPostgresData() {
    const client = await pool.connect();
    if (client === undefined)
        return console.error('Error acquiring client');
    try {
        const queryText = 'SELECT * FROM accounts as a JOIN tweets as t ON a.id = t.author_id WHERE t.compound > 0.5 OR t.compound < -0.5 LIMIT 5';
        const result = await client.query(queryText);
        console.log(result.rows.length);
        return result.rows;
    } catch (e) {
        throw e;
    } finally {
        client.release();
    }
}

async function convertToMongo(db, row) {
    // let acc = {
    //     pgID: row['id'],
    //     screen_name: row['screen_name'],
    //     name: row['name'],
    //     description: row['description'],
    //     folCount: row['followers_count'],
    //     friendCount: row['friends_count'],
    //     statCount: row['statuses_count']
    // }
    // let tweet = {
    //     pgID: row['id'],
    //     content: row['content'],
    //     location: row['geometry'],
    //     retCount: row['retweet_count'],
    //     favCount: row['favorite_count'],
    //     compound: row['compound'],
    //     happenedAt: row['happened_at'],
    //     authorID: row['author_id'],
    //     countryID: row['country_id'],
    //     parentID: row['parent_id']
    // }
    // await db.collection('authors').insertOne(acc)
    // await db.collection('tweets').insertOne(tweet)
    await db.collection('authors').updateOne({ pgID: acc.pgID }, { $set: acc }, { upsert: true })
    await db.collection('tweets').updateOne({ pgID: acc.pgID }, { $set: tweet }, { upsert: true })
}

// let data = await loadPostgresData();
// console.log(data)

// for(let i in data){
//     console.log(i);
//     convertToMongo(i);
// }

function createMongoAccount(row) {
    return {
        pgID: row['id'],
        screen_name: row['screen_name'],
        name: row['name'],
        description: row['description'],
        folCount: row['followers_count'],
        friendCount: row['friends_count'],
        statCount: row['statuses_count']
    }
}

function createMongoTweet(row) {
    return {
        pgID: row['id'],
        content: row['content'],
        location: row['geometry'],
        retCount: row['retweet_count'],
        favCount: row['favorite_count'],
        compound: row['compound'],
        happenedAt: row['happened_at'],
        authorID: row['author_id'],
        countryID: row['country_id'],
        parentID: row['parent_id']
    }
}

async function execute() {
    const db = await client.db(mongoDbName);
    // let test = await client.db('database').collection('comments').findOne({});
    // console.log(test);
    let data = await loadPostgresData();
    let result = data.map(val => {
        return [createMongoAccount(val), createMongoTweet(val)]
    })
    console.log(result);
    // await Promise.all(data.map(async val => {
    //     await convertToMongo(db, val);
    // }))
    process.exit(1);
}

execute();





