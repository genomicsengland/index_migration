# purpose of this script is to compare the contents of two supposedly identical
# copies of the same db. Being used to check that UKC > AWS migration of index
# database was faithful
# needs .env file with 
# A_NAME
# A_HOST
# A_PORT
# A_USER
# A_PWD
# in it and then also B_ equivalents
# then a text files dbs.txt which lists the databases of interest
rm(list = objects())
options(stringsAsFactors = FALSE,
    scipen = 200)
library(dotenv)
library(DBI)
library(RSQLite)

# list of dbs to be investigated across instances
# any to be skipped, just prefix with #
dbs <- read.table("dbs.txt")[[1]]

# generate the connection to given db in given instance
get_db_con <- function(db, instance_alias) {
    dbConnect(RPostgres::Postgres(),
              dbname = db,
              host = Sys.getenv(paste0(instance_alias, "_HOST")),
              port = Sys.getenv(paste0(instance_alias, "_PORT")),
              user = Sys.getenv(paste0(instance_alias, "_USER")),
              password = Sys.getenv(paste0(instance_alias, "_PWD")))
}

# get all the non-system schemas, tables and columns getting a single example
# of each datatype in the table
get_schemas_tables_and_columns <- function(con) {
    d <- dbGetQuery(con, "
    with all_cols as (
        select t.table_catalog as db
            ,t.table_schema as schema
            ,t.table_name as table
            ,c.column_name as column
            ,c.data_type
            ,row_number() over(partition by c.data_type order by random()) as rn
        from information_schema.tables t
        join information_schema.columns c
        on t.table_schema = c.table_schema and t.table_name = c.table_name 
        where t.table_type = 'BASE TABLE' and
        t.table_schema not in ('pg_catalog', 'information_schema') and 
        c.data_type not in ('xml', 'unknown', 'json')
    )
    select * from all_cols where rn = 1
    ;")
    return(d)
}

# get an ordered single column from a instance of a database
get_column <- function(con, schema, table, column) {
    sql <- paste0('select "', column, '" from "', schema, '"."', table,
                  '" order by "', column, '";')
    tryCatch({
        d <- dbGetQuery(con, sql)
        return(d[[1]])},
        error = function(e) {
            d <- c(NA)
            return(d)
        })
}

# compare two columns across instances
compare_column_across_aliases <- function(con_1, con_2, schema, table, column) {
    col_1 <- get_column(con_1, schema, table, column)
    col_2 <- get_column(con_2, schema, table, column)
    return(identical(col_1, col_2))
}

# compare random set of columns in databases in two instances of postgres
compare_random_columns_in_db <- function(db, instance_aliases) {
    cat(paste("\n-- sourcing columns from", db))
    # get the two connections
    con_1 <- get_db_con(db, instance_aliases[1])
    con_2 <- get_db_con(db, instance_aliases[2])
    # get the set of columns to be worked through
    cols <- get_schemas_tables_and_columns(con_1)
    if (nrow(cols) > 0) {
        # set-up output database
        out <- data.frame(matrix(ncol = 6, nrow = nrow(cols)))
        colnames(out) <- c("db", "schema", "table", "column", "data_type",
                           "identical")
        for (i in seq_len(nrow(cols))) {
            schema <- cols$schema[i]
            table <- cols$table[i]
            column <- cols$column[i]
            data_type <- cols$data_type[i]
            col_1 <- get_column(con_1, schema, table, column)
            col_2 <- get_column(con_2, schema, table, column)
            out$db[i] <- db
            out$schema[i] <- schema
            out$table[i] <- table
            out$column[i] <- column
            out$data_type[i] <- data_type
            out$identical[i] <- identical(col_1, col_2)
        }
        return(out)
    }
}

d <- lapply(dbs, FUN = function(x) {
    d <- compare_random_columns_in_db(x, c("A", "B"))
    }
)

names(d) <- dbs

lapply(d, function(x) {table(x$identical)})

sqlite_con <- dbConnect(SQLite(), dbname = "index_compare.db")
dbWriteTable(sqlite_con, "compare_cols", d[[1]], overwrite = T, row.names = F)
