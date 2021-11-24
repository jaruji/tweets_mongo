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
    //get all extreme tweets
    const client = await pool.connect();
    if (client === undefined)
        return console.error('Error acquiring client');
    try {
        const queryText = `SELECT a.id as acc_id, t.id as tweet_id, * 
                        FROM accounts as a 
                        JOIN tweets as t ON a.id = t.author_id 
                        WHERE t.compound > 0.5 OR t.compound < -0.5`;
        const result = await client.query(queryText);
        console.log(result.rows.length);
        return result.rows;
    } catch (e) {
        throw e;
    } finally {
        client.release();
    }
}

async function loadHashtagData() {
    //get all extreme tweet hashtags
    const client = await pool.connect();
    if (client === undefined)
        return console.error('Error acquiring client');
    try {
        const queryText = `SELECT t.id as tweet_id, h.id as hashtag_id, h.value as value
                        FROM tweets as t 
                        JOIN tweet_hashtags as th ON th.tweet_id = t.id
                        JOIN hashtags as h ON th.hashtag_id = h.id
                        WHERE t.compound > 0.5 OR t.compound < -0.5`;
        const result = await client.query(queryText);
        console.log(result.rows.length);
        return result.rows;
    } catch (e) {
        throw e;
    } finally {
        client.release();
    }
}


async function convertToMongo(db, accs, tweets) {
    db.collection('authors').createIndex({ pgID: 1 }, { unique: true }) //make it so pgID is unique
    db.collection('tweets').createIndex({ pgID: 1 }, { unique: true }) //same thing
    await db.collection('authors').insertMany(accs, { ordered: false }).catch(err => { })
    await db.collection('tweets').insertMany(tweets, { ordered: false }).catch(err => { })
}

function createMongoAccount(row) {
    return {
        pgID: row['acc_id'],
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
        pgID: row['tweet_id'],
        content: row['content'],
        location: row['geometry'],
        retCount: row['retweet_count'],
        favCount: row['favorite_count'],
        compound: row['compound'],
        happenedAt: row['happened_at'],
        authorID: row['author_id'],
        countryID: row['country_id'],
        parentID: row['parent_id'],
        hashtags: []
    }
}

async function execute() {
    const db = await client.db(mongoDbName);
    // let test = await client.db('database').collection('comments').findOne({});
    // console.log(test);
    let data = await loadPostgresData();
    let accs = data.map(val => {
        return createMongoAccount(val);
    })
    let tweets = data.map(val => {
        return createMongoTweet(val);
    })
    data = null;
    await convertToMongo(db, accs, tweets);
    console.log('Data stored in mongo')
    accs = null;
    tweets = null;
    console.log("tweets uploaded");
    process.exit(1);
}


async function updateTweets(){
    const db = await client.db(mongoDbName);
    data = await loadHashtagData();
    //console.log(data.slice(0,10));
    console.log('Loaded hashtag data')
    for(let val of data){
        await db.collection('tweets').updateOne( {pgID: val['tweet_id']}, {$push: {hashtags: {value: val['value'], hashtagID: val['hashtag_id']}}})
    }
    console.log("Hashtags updated.");
    process.exit(1);
}

// execute();
updateTweets();





