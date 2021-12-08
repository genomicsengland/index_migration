rm(list = objects())
library(DBI)
ukc <- readRDS("rc_hash_UKC.rds")
aws <- readRDS("rc_hash_AWS.rds")

ukc <- do.call("rbind", ukc)
aws <- do.call("rbind", aws)

d <- merge(ukc, aws, by = c("db", "schema", "table"), all = T, suffixes = c("_ukc", "_aws"))

sqlite_con <- dbConnect(RSQLite::SQLite(), dbname = "index_compare.db")
dbWriteTable(sqlite_con, "hash_rc", d, overwrite = T, row.names = F)
