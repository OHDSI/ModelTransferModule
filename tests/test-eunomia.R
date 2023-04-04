library(testthat)
library(Eunomia)
connectionDetails <- getEunomiaConnectionDetails()
Eunomia::createCohorts(connectionDetails = connectionDetails)

workFolder <- tempfile("work")
dir.create(workFolder)
resultsfolder <- tempfile("results")
dir.create(resultsfolder)
jobContext <- readRDS("tests/testJobContext.rds")
jobContext$moduleExecutionSettings$workSubFolder <- workFolder
jobContext$moduleExecutionSettings$resultsSubFolder <- resultsfolder
jobContext$moduleExecutionSettings$connectionDetails <- connectionDetails

test_that("Run module", {
  source("Main.R")
  execute(jobContext)
  resultsFiles <- list.files(resultsfolder)
  expect_true("github_export.csv" %in% resultsFiles)
  expect_true(dir.exists(file.path(workFolder, 'models','model_github_1_1')))
  expect_true(length(list.files(file.path(workFolder, 'models','model_github_1_1')))>0)
  
})

unlink(workFolder)
unlink(resultsfolder)
unlink(connectionDetails$server())
