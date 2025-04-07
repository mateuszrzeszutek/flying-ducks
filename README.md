# Flying Ducks DB

This is a demo project showing how to implement a Flight SQL database server.
It uses DuckDB internally to run the queries on an example database file.

## Prepare the test database

1. [Install DuckDB on your machine](https://duckdb.org/docs/installation/). For example, on macOS with Homebrew:

    ```shell
    brew install duckdb
    ```

2. Download one of the example NYC trips data file and unzip it:

    ```shell
    curl https://blobs.duckdb.org/data/nyc-taxi-dataset/trips_xaa.csv.gz -o data.csv.gz
    gunzip data.csv.gz
    ```

3. Create a new `database.duckdb` file:

    ```shell
    duckdb database.duckdb
    ```

4. In the DuckDB shell, import schema:

    ```sql
    .read schema.sql
    COPY trips FROM 'data.csv' (HEADER false);
    ```

## Build and run the application

1. Build the app:

    ```shell
    ./gradlew build
    ```

2. And run it on the prepared database file:

    ```shell
    ./gradlew run --args="$(pwd)/database.duckdb"
    ```

## Query the FlyingDucks server

1. Download the Arrow Flight SQL JDBC driver:

    ```shell
    curl https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/18.2.0/flight-sql-jdbc-driver-18.2.0.jar\
       -o flight-sql-jdbc-driver.jar
    ```

### IntelliJ Ultimate

1. Open the `Database` tab, then click the plus sign and choose `Driver`.
   Use the `Driver Files` input to find and add the file that you've just downloaded.
   In the `Class` combo box select the `org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver`.
   Save the newly created driver.
2. Using the plus sign again, create a new `Data Source` choosing the driver you just saved. Set the following options:
   - Authentication: No auth
   - URL: `jdbc:arrow-flight-sql://localhost:7777?useEncryption=false`
3. Save the data source -- you should be able to inspect the database schema and query it now.

### VSCode/VSCodium

1. Install the `Database Client` and `Database Client JDBC` extensions.
2. In the `Database` tab, click `Add Connection`
3. Enter the following information:
   - JDBC URL: `jdbc:arrow-flight-sql://localhost:7777?useEncryption=false`
   - Driver Path: `<path-to-the-flying-ducks-project>/flight-sql-jdbc-driver.jar`
   - Dialect: `PostgreSQL (Compatible)`
4. Click `Save` -- you should be able to inspect the database schema and query it now.

# TODO list

- [ ] sql info to be compatible with intellij
- [ ] prepared statements
- [ ] prepared statement parameters
- [x] schemas filters
- [x] tables filters
- [x] columns schema discovery
- [ ] updates
- [ ] jdbc driver integration test
- [ ] adbc driver integration test