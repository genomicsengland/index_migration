#-- get all the dbs, schemas and tables from index
rm(list = objects())
options(stringsAsFactors = FALSE,
    scipen = 200)
library(DBI)
library(dotenv)

# list of dbs to be investigated across instances
# any to be skipped, just prefix with #
dbs <- read.table("dbs.txt")[[1]]
ignore <- read.table("ignore.txt")[[1]]

# generate the connection to given db in given instance
get_db_con <- function(db, instance_alias) {
    dbConnect(RPostgres::Postgres(),
              dbname = db,
              host = Sys.getenv(paste0(instance_alias, "_HOST")),
              port = Sys.getenv(paste0(instance_alias, "_PORT")),
              user = Sys.getenv(paste0(instance_alias, "_USER")),
              password = Sys.getenv(paste0(instance_alias, "_PWD")))
}

# get all the non-system schemas and tables at a connection
get_schemas_and_tables <- function(con) {
    d <- dbGetQuery(con, "
        select table_catalog as db
            ,table_schema as schema
            ,table_name as table
        from information_schema.tables
        where table_type = 'BASE TABLE' and 
        table_schema not in ('pg_catalog', 'information_schema')
        ;")
    return(d)
}
 
d <- list()

for(i in seq_along(dbs)) {
    con <- get_db_con(dbs[i], "A")
    d[[i]] <- get_schemas_and_tables(con)
}

e <- do.call("rbind", d)
e$address <- paste(e$db, e$schema, e$table, sep = ".")
e$ignore <- e$address %in% ignore
write.csv(e, "tab_manifest.csv", row.names = FALSE)
