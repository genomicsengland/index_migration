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
 
# list of dbs to be investigated across instances, hash prefix will exclude
dbs <- read.table("dbs.txt")[[1]]
fldr <- "~/scr/migr_rds"
dir.create(fldr)

tab_manifest <- read.csv("tab_manifest.csv")
tab_manifest <- tab_manifest[!tab_manifest$ignore,]
 
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
#get_schemas_and_tables <- function(con) {
#    d <- dbGetQuery(con, "
#        select table_catalog as db
#            ,table_schema as schema
#            ,table_name as table
#        from information_schema.tables
#        where table_type = 'BASE TABLE' and 
#        table_schema not in ('pg_catalog', 'information_schema')
#        ;")
#    return(d)
#}
 
# get the row count for schema and table a connection. Needs the double quotes
# around schema and table as some don't play nice with DBI
get_row_count <- function(con, schema, table) {
    sql <- paste0('select count(*)::int from "', schema, '"."', table, '";')
    d <- dbGetQuery(con, sql)$count
    return(d)
}
 
# get a hash for the whole table at a connection
get_table_hash <- function(con, schema, table) {
   cat(paste("\n>>>", paste0(schema, ".", table)))
     sql <- paste0('with d as ( select md5(d::text) as hash from "',
         schema,
         '"."',
         table,
         '" d order by d limit 100000) select md5(array_agg(hash order by hash)::varchar) as h from d ')
     tryCatch({
         d <- dbGetQuery(con, sql)$h
         return(d)
     }, error = function(e) {
         sql <- paste0('with d as ( select md5(d::text) as hash from "',
             schema,
             '"."',
             table,
             '" d) select md5(array_agg(hash order by hash)::varchar) as h from d ')
         d <- dbGetQuery(con, sql)$h
         return(d)
     })
}

# set a given parameter for the session
set_session_config <- function(con, parameter, value) {
    sql  <- paste0('set ', parameter, ' to ', value, ';')
    d <- dbGetQuery(con, sql)
    return(d)
}
 
# put functions together to get row counts and hashes for every table in every
# schema at a given connection
get_rc_and_hash_in_db_instance <- function(con, db, instance_name) {
    d <- tab_manifest[tab_manifest$db == db, c("db", "schema", "table")]
    d$row_count <- c(NA)
    d$hash <- c(NA)
    for (r in seq_len(nrow(d))) {
        d$row_count[r] <- get_row_count(con, d$schema[r], d$table[r])
        d$hash[r] <- get_table_hash(con, d$schema[r], d$table[r])
    }
    saveRDS(d, paste0(fldr, "/_", db, "_", instance_name, ".rds"))
    return(d)
}
 
# get the names from the dotenv for each instance
get_instance_names <- function(aliases) {
    sapply(aliases, function(x) {
               Sys.getenv(paste0(x, "_NAME"))
    })
}
 
# run the above on a given database across the different aliases
get_rc_and_hash_across_db_instances <- function(db, instance_aliases) {
 
    # get the names of the different instances
    instance_names <- get_instance_names(instance_aliases)
 
    # prepare output df
    out <- data.frame()
 
    for (i in seq_along(instance_names)) {
        cat(paste0("\n-- sourcing data from ", db, " on ", instance_names[i]))
        con <- get_db_con(db, instance_aliases[i])
        set_session_config(con, "extra_float_digits", -15)
        d <- get_rc_and_hash_in_db_instance(con, db, instance_names[i])
        # if this is the first instance then create the df
        if (i == 1) {
            out <- d
        } else {
            # otherwise merge it in using the instance names as suffixes
            out  <- merge(out, d,
                          by = c("db", "schema", "table"),
                          suffixes = instance_names[1:i], all = TRUE)
        }
    }
    return(out)
}
 
d <- lapply(dbs, FUN = function(x) {
                get_rc_and_hash_across_db_instances(x, c("A", "B"))
                          })
 
# sqlite_con <- dbConnect(RSQLite::SQLite(), dbname = "index_compare.db")
# dbWriteTable(sqlite_con, "hash_rc", d[[1]], overwrite = T, row.names = F)
saveRDS(d, "rc_hash.rds")
