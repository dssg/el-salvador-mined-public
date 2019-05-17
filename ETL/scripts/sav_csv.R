#!/usr/bin/env Rscript

# Arguments:
#  args[1]: input file path
#  args[2]: output file path

args = commandArgs(trailingOnly=TRUE)

library(foreign)
print(args[1])
write.table(read.spss(args[1], use.value.labels=FALSE), file=args[2], quote = TRUE, sep = "^", na='', row.names = FALSE)

# reencode='LATIN1' fileEncoding="LATIN1"