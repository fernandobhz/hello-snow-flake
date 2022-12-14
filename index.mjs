import snowflake from "snowflake-sdk";

console.log(new Date(), 'starting');

const account = process.env.snowflakeaccount;
const username = process.env.snowflakeusername;
const password = process.env.snowflakepassword;
const application = process.env.snowflakeapplication;
const warehouse = process.env.snowflakewarehouse;

const connection = snowflake.createConnection({
  account,
  username,
  password,
  warehouse,
  application,
});

connection.connect(function (err, conn) {
  if (err) {
    console.error("Error " + err.message);
    process.exit(err.number || 1);
  }

  const sqlText = `
    select * 
    from SNOWFLAKE_SAMPLE_DATA.tpch_sf1.customer 
    limit 10;
  `;

  connection.execute({
    sqlText,
    complete: function (err, stmt, rows) {
      if (err) {
        console.error("Error " + err.message);
        process.exit(err.number || 1);
      }

      console.log(new Date(), 'done');
      console.log(rows);
    },
  });
});
