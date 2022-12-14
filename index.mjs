import snowflake from "snowflake-sdk";
import stream from "stream";
import fs from "fs/promises";
import util from "util";

class SnowFlakeFacade {
  static connect({ account, username, password, warehouse, application }) {
    const conn = snowflake.createConnection({
      account,
      username,
      password,
      warehouse,
      application,
    });

    return util.promisify(conn.connect).call(conn);
  }

  static execute(rawConnection, sqlText) {
    return new Promise((resolve, reject) => {
      rawConnection.execute({
        sqlText,
        complete: (error, statement, rows) => {
          if (error) {
            return reject(error);
          }

          resolve(rows);
        },
      });
    });
  }

  constructor({
    account,
    username,
    password,
    warehouse,
    application,
    autoConnect = true,
  }) {
    this.account = account;
    this.username = username;
    this.password = password;
    this.warehouse = warehouse;
    this.application = application;
    this.connected = false;
    this.connecting = null;

    this.connection = snowflake.createConnection({
      account,
      username,
      password,
      warehouse,
      application,
    });

    if (autoConnect) {
      this.connect();
    }
  }

  raw() {
    return this.connection;
  }

  connect() {
    if (this.connecting) {
      return this.connecting;
    }

    this.connecting = new Promise(async (resolve, reject) => {
      await util.promisify(this.connection.connect).call(this.connection);
      this.connected = true;
      resolve(this);
    });

    return this.connecting;
  }

  async query(sqlText) {
    if (!this.connected) {
      await this.connect();
    }
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText,
        complete: (error, statement, rows) => {
          if (error) {
            return reject(error);
          }

          resolve(rows);
        },
      });
    });
  }
}

export const pipelineAsPromise = (...streamList) => {
  return new Promise((resolve, reject) => {
    const pipeline = new stream.pipeline(...streamList, (error, ...args) => {
      if (error) {
        return reject(error);
      }

      return resolve(...args);
    });
  });
};

export class CurrentDateTimeReader extends stream.Readable {
  constructor(options) {
    super(options);
    this.counter = 0;
  }
  _read() {
    this.counter++;

    if (this.counter === 100) {
      this.push(null);
      return;
    }

    this.push(new Date().toISOString());
  }
}

export class OutputTxtWritable extends stream.Writable {
  constructor(options = {}) {
    super({ ...options, objectMode: true });
    this.count = 0;
  }
  async _write(chunk, encoding, callback) {
    this.count++;
    const data = `${
      this.count
    } - ${new Date().toISOString()} - ${encoding} - ${JSON.stringify(chunk)}`;
    console.log(data);
    await fs.appendFile("Output.txt", data);
    callback();
  }
}

export class SnowFlakeReader extends stream.Readable {
  static async connect({ account, username, password, warehouse, application }) {
    const conn = snowflake.createConnection({
      account,
      username,
      password,
      warehouse,
      application,
    });

    await util.promisify(conn.connect).call(conn);
    return conn;
  }

  constructor({ conn, commandText, params = [], streamOptions = {} }) {
    super({ ...streamOptions, objectMode: true });
    this.conn = conn;
    this.commandText = commandText;
    this.params = params;

    this.query = null;
    this.started = false;
    this.paused = false;
  }

  start() {
    const sqlText = this.commandText;
    const statement = this.conn.execute({ sqlText });

    this.query = statement.streamRows();
    this.query.on("data", this.onRow.bind(this));
    this.query.on("error", this.destroy.bind(this));
    this.query.on("end", this.finish.bind(this));

    this.started = true;
  }
  onRow(row) {
    const pushResponse = this.push(row);

    if (!pushResponse) {
      this.paused = true;
      this.conn.pause();
    }
  }
  continue() {
    this.conn.resume();
    this.paused = false;
  }
  _read(size) {
    if (!this.started) {
      return this.start();
    }

    if (this.paused) {
      return this.continue();
    }
  }
  finish() {
    this.started = false;
    this.paused = false;
    this.push(null);
  }
}

const { log } = console;

log(new Date(), "starting");

const account = process.env.snowflakeaccount;
const username = process.env.snowflakeusername;
const password = process.env.snowflakepassword;
const application = process.env.snowflakeapplication;
const warehouse = process.env.snowflakewarehouse;

const conn = new SnowFlakeFacade({
  account,
  username,
  password,
  warehouse,
  application,
});

const sqlText = `
  select *
  from SNOWFLAKE_SAMPLE_DATA.tpch_sf1.customer
  limit 10;
`;

log(await conn.query(sqlText));

console.time("pipePromise");
const snowFlakeRawConnection = await SnowFlakeReader.connect({
  account,
  username,
  password,
  warehouse,
  application,
});

const snowflakeReader = new SnowFlakeReader({
  conn: snowFlakeRawConnection,
  commandText: sqlText,
});

const outputTxtWritable = new OutputTxtWritable();

await pipelineAsPromise(snowflakeReader, outputTxtWritable);
console.timeEnd("pipePromise");
