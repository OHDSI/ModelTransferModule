# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of ModelTransferModule
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Module methods -------------------------
getModuleInfo <- function() {
  checkmate::assert_file_exists("MetaData.json")
  return(ParallelLogger::loadSettingsFromJson("MetaData.json"))
}


# Module methods -------------------------
execute <- function(jobContext) {
  rlang::inform("Validating inputs")
  inherits(jobContext, "list")

  if (is.null(jobContext$settings)) {
    stop("Analysis settings not found in job context")
  }
  if (is.null(jobContext$sharedResources)) {
    stop("Shared resources not found in job context")
  }
  if (is.null(jobContext$moduleExecutionSettings)) {
    stop("Execution settings not found in job context")
  }

  resultsFolder <- jobContext$moduleExecutionSettings$resultsSubFolder

  modelSaveLocation <- jobContext$moduleExecutionSettings$workSubFolder 

  rlang::inform("Transfering models")

  # finding S3 details
  s3Settings <- jobContext$settings$s3Settings
  # finding github details
  githubSettings <- jobContext$settings$githubSettings
  # finding localFile details
  localFileSettings <- jobContext$settings$localFileSettings

  modelLocationsS3 <- tryCatch(getModelsFromS3(
    s3Settings = s3Settings,
    saveFolder = modelSaveLocation
  ), error = function(e) {
    ParallelLogger::logInfo(e)
    return(NULL)
  })
  if (!is.null(modelLocationsS3)) {
    readr::write_csv(modelLocationsS3,
      file = file.path(resultsFolder, "s3_export.csv")
    )
  }

  modelLocationsGithub <- tryCatch(
    {
      getModelsFromGithub(
        githubSettings = githubSettings,
        saveFolder = modelSaveLocation
      )
    },
    error = function(e) {
      ParallelLogger::logInfo(e)
      return(NULL)
    }
  )
  if (!is.null(modelLocationsGithub)) {
    readr::write_csv(modelLocationsGithub,
      file = file.path(resultsFolder, "github_export.csv"))
  }

  modelLocationsLocalFiles <- tryCatch(
    {
      getModelsFromLocalFiles(
        localFileSettings = localFileSettings$locations,
        saveFolder = modelSaveLocation
      )
    },
    error = function(e) {
      ParallelLogger::logInfo(e)
      return(NULL)
    }
  )
  if (!is.null(modelLocationsS3)) {
    readr::write_csv(modelLocationsLocalFiles,
      file = file.path(resultsFolder, "local_export.csv"))
  }
}

getModelsFromLocalFiles <- function(
    localFileSettings,
    saveFolder) {
  if (is.null(localFileSettings)) {
    return(NULL)
  }

  if (!fs::dir_exists(fs::path(saveFolder, "models"))) {
    dir.create(fs::path(saveFolder, "models"), recursive = TRUE)
  }

  saveFolder <- fs::path(saveFolder, "models")

  localFileSettings <- fs::path_expand(localFileSettings)
  saveFolder <- fs::path_expand(saveFolder)

  contents <- fs::dir_ls(localFileSettings)

  for (item in contents) {
    # Determine the target path in the destination folder
    targetPath <- fs::path(saveFolder, fs::path_file(item))
    # Copy the item to the destination
    if (fs::dir_exists(item)) {
      dir.create(targetPath) # Ensure the directory exists before copying into it
      fs::dir_copy(item, targetPath)
    } else {
      fs::file_copy(item, targetPath)
    }
  }

  info <- data.frame()
  return(info)
}

#' code that takes s3 details and download the models and returns the locations plus details as data.frame
#' need to have settings
#' AWS_ACCESS_KEY_ID=<my access key id>
#' AWS_SECRET_ACCESS_KEY=<my secret key>
#' AWS_DEFAULT_REGION=ap-southeast-2
getModelsFromS3 <- function(
    s3Settings,
    saveFolder) {

  if (is.null(s3Settings)) {
    return(NULL)
  }

  info <- data.frame()

  for (i in seq_len(nrow(s3Settings))) {
    modelSaved <- FALSE
    saveToLoc <- ""

    validBucket <- aws.s3::bucket_exists(
      bucket = s3Settings$bucket[i],
      region = s3Settings$region[i]
    )

    if (validBucket) {
      subfolder <- s3Settings$modelZipLocation[i]
      bucket <- s3Settings$bucket[i]
      region <- s3Settings$region[i]

      result <- aws.s3::get_bucket_df(bucket = bucket, region = region, max = Inf)
      paths <- fs::path(result$Key)

      workDir <- findWorkDir(bucket, subfolder, region)
      analyses <- findAnalysesNames(bucket, workDir, region)

      if (length(analyses) > 0) {
        if (!fs::dir_exists(fs::path(saveFolder, "models"))) {
          dir.create(fs::path(saveFolder, "models"), recursive = TRUE)
        }
        saveToLoc <- fs::path(saveFolder, "models")

        for (analysis in analyses) {
          analysisPaths <- paths[fs::path_has_parent(paths,
            fs::path(workDir, analysis))]

          for (obj in analysisPaths) {
            # split work directory from path
            relativePaths <- fs::path_rel(obj, start = workDir)
            # remove artifacts created by current path location
            filteredPaths <- relativePaths[relativePaths != "."]
            # Construct the file path where you want to save the file locally
            localFilePath <- fs::path(saveToLoc, filteredPaths)

            # Download the file from S3
            aws.s3::save_object(obj, bucket, file = localFilePath)
          }
          ParallelLogger::logInfo(paste0("Downloaded: ", analysis, " to ", saveToLoc))
        }
      } else {
        ParallelLogger::logInfo(paste0("No ", s3Settings$modelZipLocation[i],
          " in bucket ", s3Settings$bucket[i], " in region ", s3Settings$region[i]))
      }
    } else {
      ParallelLogger::logInfo(paste0("No bucket ", s3Settings$bucket[i],
        " in region ", s3Settings$region[i]))
    }

    info <- rbind(
      info,
      data.frame(
        originalLocation = "PLACEHOLDER",
        modelSavedLocally = TRUE,
        localLocation = saveToLoc
      )
    )
  }

  return(info)
}

# code that takes github details and download the models and returns the locations plus details as data.frame
getModelsFromGithub <- function(
    githubSettings,
    saveFolder) {
  if (is.null(githubSettings)) {
    return(NULL)
  }

  info <- data.frame()
  for (i in seq_len(nrow(githubSettings))) {
    user <- githubSettings[i, ]$user #' ohdsi-studies'
    repository <- githubSettings[i, ]$repository #' lungCancerPrognostic'
    ref <- githubSettings[i, ]$ref #' master'

    downloadCheck <- tryCatch(
      {
        utils::download.file(
          url = file.path(
            "https://github.com", user, repository, "archive",
            paste0(ref, ".zip")
          ),
          destfile = file.path(tempdir(), "tempGitHub.zip")
        )
      },
      error = function(e) {
        ParallelLogger::logInfo("GitHub repository download failed")
        return(NULL)
      }
    )

    if (!is.null(downloadCheck)) {
      # unzip into the workFolder
      OhdsiSharing::decompressFolder(
        sourceFileName = file.path(tempdir(), "tempGitHub.zip"),
        targetFolder = file.path(tempdir(), "tempGitHub")
      )
      for (j in 1:length(githubSettings[i, ]$modelsFolder)) {
        modelsFolder <- githubSettings[i, ]$modelsFolder[j] #' models'
        modelFolder <- if (is.null(githubSettings[i, ]$modelFolder[j])) "" else githubSettings[i, ]$modelFolder[j] #' full_model'

        tempModelLocation <- file.path(tempdir(),
          "tempGitHub",
          paste0(repository, "-", ref),
          "inst",
          modelsFolder,
          modelFolder
        )

        if (!dir.exists(file.path(saveFolder, "models",
          paste0("model_github_", repository, "_", ref)))) {
          dir.create(file.path(saveFolder, "models",
            paste0("model_github_", repository, "_", ref)), recursive = TRUE)
        }
        for (dirEntry in dir(tempModelLocation)) {
          file.copy(
            from = file.path(tempModelLocation, dirEntry),
            to = file.path(saveFolder, "models", paste0("model_github_", repository, "_", ref)), # issues if same modelFolder name in different github repos
            recursive = TRUE
          )
        }

        modelSaved <- TRUE
        saveToLoc <- file.path(saveFolder, "models", paste0("model_github_", repository, "_", ref))

        info <- rbind(
          info,
          data.frame(
            githubLocation = file.path(
              "https://github.com", user,
              repository, "archive", paste0(ref, ".zip")
            ),
            githubPath = file.path("inst", modelsFolder, modelFolder),
            modelSavedLocally = modelSaved,
            localLocation = saveToLoc
          )
        )
      }
    } else {
      info <- rbind(
        info,
        data.frame(
          githubLocation = file.path(
            "https://github.com", user, repository,
            "archive", paste0(ref, ".zip")
          ),
          githubPath = file.path("inst", githubSettings[i, ]$modelsFolder,
            githubSettings[i, ]$modelFolder),
          modelSavedLocally = FALSE,
          localLocation = ""
        )
      )
    }
  }

  return(info)
}

findWorkDir <- function(bucket, subfolder, region) {
  # list all content in the bucket
  result <- aws.s3::get_bucket_df(bucket = bucket, region = region, max = Inf)
  # extract paths of all content
  paths <- fs::path(result$Key)
  # split paths up for easier processing
  splitPath <- fs::path_split(paths)

  # find the full path of the subfolder with models for validation
  resultsSapply <- sapply(splitPath, function(x) {
    identical(tail(x, 1), subfolder)
  })
  subfolderPath <- paths[resultsSapply]

  return(subfolderPath)
}

findAnalysesNames <- function(bucket, workDir, region) {
  # list all content in the bucket
  result <- aws.s3::get_bucket_df(bucket = bucket, region = region, max = Inf)
  # extract paths of all content
  paths <- fs::path(result$Key)
  # filter for paths in work directory
  workDirPaths <- paths[fs::path_has_parent(paths, workDir)]
  # split work directory from path
  relativePaths <- fs::path_rel(workDirPaths, start = workDir)
  # remove artifacts created by current path location
  filteredPaths <- relativePaths[relativePaths != "."]
  # get only the top level directories
  topLevelDirs <- sapply(fs::path_split(filteredPaths), function(p) p[[1]])
  topLevelDirs <- unique(topLevelDirs)
  return(topLevelDirs)
}
