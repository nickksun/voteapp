// const Backoff = require('./Backoff');
const defaults = require('./defaults');
// const mongodb = require('mongodb');
const uuid = require('uuid/v1');

const AWS = require('aws-sdk');
const AWS_DEPLOY_REGION = process.env.AWS_DEPLOY_REGION;
const dynamoDb = new AWS.DynamoDB.DocumentClient({
  api_version: '2012-08-10',
  region: AWS_DEPLOY_REGION
});

const Client = mongodb.MongoClient;

// The `votes` collection
const VOTES = 'votes';

class Database {
  /**
   * Create a new Database instance.
   * @param {object} [config] Object with valid url or uri property for connection string, or
   *                        else host, port, and db properties. Can also have an options property.
   * @throws {Error} if invalid config is provided.
   */
  constructor(config) {
    this._client = dynamoDb;
    // this._isConnected = false;
    this._config = Object.assign(Database.defaults().config(), config || {});
    // checkConfig(this._config);
  }

  /**
   * Get a copy of the database defaults object
   * @return {{}}
   */
  static defaults() {
    return Object.assign({}, defaults);
  }

  /**
   * Creates a config object initialized with the defaults, then overridden the following
   * environment variables, then finally overridden by any explicit props set by the 
   * supplied config object.
   * For environment variables, it checks first for DATABASE_URI and sets the uri property;
   * else if not present, then checks for DATABASE_HOST and DATABASE_PORT and sets the
   * host and port properties.
   * @param {object} config, a configuration object with properties that override all else.
   * @returns {{}}
   */
  static createStdConfig(config) {
    let c = Database.defaults().config();

    // if (process.env.DATABASE_URI) {
    //   c.uri = process.env.DATABASE_URI;
    //   delete c.host;
    // } else {
    //   c.host = process.env.DATABASE_HOST || c.host;
    //   c.port = process.env.DATABASE_PORT || c.port;
    // }
    c.db = process.env.DATABASE_NAME || c.db;

    // When connecting, we check first for a uri, so if the config object has explicitly
    // specified host and port, then we need to explicitly delete the uri property.
    // if (config && config.host && config.port) {
    //   delete c.uri;
    // }

    return Object.assign(c, config || {});
  }

  /**
   * Get a copy of the current config.
   * The config is an object with `host`, `port`, and `db` OR `uri` (or `url`) properties.
   * @return {{}}
   */
  get config() {
    return Object.assign({}, this._config);
  }

  /**
   * Get the connection URL based on the current config.
   * Returns value of url property if present, else returns value of uri property
   * if present, else returns generated string based on host, port, and db properties.
   * @return {string}
   */
  // get connectionURL() {
  //   return this.config.uri ?
  //     this.config.uri :
  //     this.config.url ?
  //       this.config.url :
  //       `mongodb://${this.config.host}:${this.config.port}/${this.config.db}`;
  // }

  /**
   * Return true if a client connection has been established, otherwise false.
   * @return {boolean}
   */
  // get isConnected() {
  //   return this._isConnected;
  // }

  /**
   * Return the actual connected client after connecting.
   * @return {*}
   */
  get client() {
    return this._client;
  }

  /**
   * Return the actual database instance after connecting.
   * @return {*}
   */
  // get instance() {
  //   return this._instance;
  // }

  /**
   * Establish a connection to the database.
   * @param options - the backoff options
   * @throws {Error} Connection error.
   * @return {Promise<void>}
   */
  async connect(options) {
    // let connectOpts = Object.assign({
    //   retryIf: err => err.name === 'MongoNetworkError' 

    // }, options);

    // if (this._isConnected) {
    //   throw new Error('Already connected');
    // }

    // let that = this;
    // let backoff = new Backoff(async () => {
    //   let opts = { useNewUrlParser: true };
    //   that._client = await Client.connect(that.connectionURL, opts);
    //   that._instance = await that._client.db(that.config.db);
    //   that._isConnected = true;
    // }, connectOpts);

    // await backoff.connect();
    return new Promise( (resolve, reject) => {
      try {
        resolve('connect ok');
      } catch (err) {
        reject(err);
      }
    });
  }
  

  /**
   * Close the connection to the database.
   * @throws {Error} Connection error.
   * @return {Promise<void>}
   */
  async close() {
    // if (this._client) {
    //   await this._client.close();
    //   this._client = null;
    //   this._instance = null;
    // }
    // this._isConnected = false;
    return new Promise( (resolve, reject) => {
      try {
        resolve('close ok');
      } catch (err) {
        reject(err);
      }
    });
  }

  /**
   * Insert or update a vote and return the new/updated doc including voter_id property.
   * @param {object} vote Must have a vote property set to either 'a' or 'b'.
   * @throws {Error} if vote is not valid.
   * @return {Promise<{}>}
   */
  async updateVote(vote) {
    // if (!this.isConnected) {
    //   throw new Error('Not connected to database');
    // }

    checkVote(vote);

    if (!vote.voter_id) {
      vote.voter_id = uuid();
    }

    const params = {
      TableName: this._config.db,
      Item: {
        voter_id: vote.voter_id,
        vote: vote.vote
      }
    }

    try {
      const data = await this.client.put(params).promise();
      return data
    } catch (error) {
      throw new Error(JSON.stringify(error));
    }


    // let col = await this.instance.collection(VOTES);
    // let result = await col.findOneAndUpdate({ voter_id: v tote.voter_id },
    //   { $set: { vote: vote.vote }},
    //   { returnOriginal: false, sort: [['voter_id',1]], upsert: true });
    // if (!result.ok) {
    //   throw new Error(JSON.stringify(result.lastErrorObject));
    // }
    // return result.value;
  }

  /**
   * Get the tally of all 'a' and 'b' votes.
   * @return {Promise<{a: number, b: number}>}
   */
  async tallyVotes() {
    try {
      const paramsA = {
        TableName: this._config.db,
        KeyConditionExpression: "#vote = :vote",
        ExpressionAttributeNames: {
          "#vote": "vote"
        },
        ExpressionAttributeValues: {
          ":vote": "a"
        }
      };
      const paramsB = {
        TableName: this._config.db,
        KeyConditionExpression: "#vote = :vote",
        ExpressionAttributeNames: {
          "#vote": "vote"
        },
        ExpressionAttributeValues: {
          ":vote": "b"
        }
      };
      let resultA = await this.client.query(paramsA).promise();
      let resultB = await this.client.query(paramsB).promise();
      return {
        a: resultA.Items.length,
        b: resultB.Items.length
      }
    } catch (error) {
      throw new Error(JSON.stringify(error));

    }
    // let col = await this.instance.collection(VOTES);
    // let count_a = await col.countDocuments({ vote: 'a' });
    // let count_b = await col.countDocuments({ vote: 'b' });
    // return {
    //   a: count_a,
    //   b: count_b
    // };
  }

}

module.exports = Database;

// validate configs before accepting
function checkConfig(c) {
  let errors = [];
  if (!c.url || !c.uri) {
    // if (!c.host) errors.push('host');
    // if (!c.port) errors.push('port');
    if (!c.db) errors.push('db');
  }
  if (errors.length) {
    // don't forget to update test if error string is updated
    throw new Error(`Invalid config. Provide a valid url (or uri) property value, or else valid values for the following: ${errors.join(', ')}`);
  }
}

// validate votes before accepting
function checkVote(vote) {
  let errors = [];
  if (!vote) {
    errors.push('missing vote');
  } else {
    if (!vote.vote) {
      errors.push('missing vote property');
    } else {
      if (vote.vote !== 'a' && vote.vote !== 'b') {
        errors.push('invalid value for vote: (must be "a" or "b")');
      }
    }
  }
  if (errors.length) {
    // don't forget to update test if error string is updated
    throw new Error(`Invalid vote: ${errors.join(', ')}`);
  }
}
