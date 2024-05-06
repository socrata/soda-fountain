// Set up the libraries
@Library('socrata-pipeline-library')

import com.socrata.ReleaseMetadataService
def rmsSupportedEnvironment = com.socrata.ReleaseMetadataService.SupportedEnvironment

// set up variables
String service = 'soda-fountain'
String project_wd = 'soda-fountain-jetty'
boolean isPr = env.CHANGE_ID != null
boolean isHotfix = isHotfixBranch(env.BRANCH_NAME)
boolean skip = false
String lastStage

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service, project_wd)
def dockerize = new com.socrata.Dockerize(steps, service, env.BUILD_NUMBER)
def releaseTag = new com.socrata.ReleaseTag(steps, service)

pipeline {
  options {
    ansiColor('xterm')
    buildDiscarder(logRotator(numToKeepStr: '50'))
    disableConcurrentBuilds(abortPrevious: true)
    timeout(time: 20, unit: 'MINUTES')
  }
  parameters {
    string(name: 'AGENT', defaultValue: 'build-worker-pg13', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build without creating a new tag.')
    string(name: 'RELEASE_NAME', defaultValue: '', description: 'For release builds, the release name which is used for the git tag and the deploy tag.')
    booleanParam(name: 'PUBLISH', defaultValue: false, description: 'Set to true to manually initiate a publish build - you must also specify PUBLISH_SHA')
    string(name: 'PUBLISH_SHA', defaultValue: '', description: 'For publish builds, the git commit SHA or branch to build from')
  }
  agent {
    label params.AGENT
  }
  environment {
    DOCKER_PATH = './docker'
    WEBHOOK_ID = 'WEBHOOK_IQ'
  }
  stages {
    stage('Publish Library') {
      when {
        expression { return params.PUBLISH }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          checkout([$class: 'GitSCM',
            branches: [[name: params.PUBLISH_SHA]],
            doGenerateSubmoduleConfigurations: false,
            gitTool: 'Default',
            submoduleCfg: [],
            userRemoteConfigs: [[credentialsId: 'a3959698-3d22-43b9-95b1-1957f93e5a11', url: 'https://github.com/socrata-platform/soda-fountain.git']]
          ])
          echo "Publishing external library"
          sbtbuild.setSubprojectName("sodaFountainExternal")
          sbtbuild.setPublish(true)
          sbtbuild.setBuildType("library")
          sbtbuild.build()
        }
      }
    }
    stage('Hotfix') {
      when {
        expression { isHotfix }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (releaseTag.noCommitsOnHotfixBranch(env.BRANCH_NAME)) {
            skip = true
            echo "SKIP: Skipping the rest of the build because there are no commits on the hotfix branch yet"
            return
          }
          env.CURRENT_RELEASE_NAME = releaseTag.getReleaseName(env.BRANCH_NAME)
          env.HOTFIX_NAME = releaseTag.getHotfixName(env.CURRENT_RELEASE_NAME)
        }
      }
    }
    stage('Build') {
      when {
        allOf {
          not { expression { return params.PUBLISH } }
          not { expression { skip } }
        }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          sbtbuild.setNoSubproject(true)
          sbtbuild.setScalaVersion("2.12")
          sbtbuild.setSubprojectName("sodaFountainJetty")
          sbtbuild.setSrcJar("soda-fountain-jetty/target/soda-fountain-jetty-assembly.jar")
          sbtbuild.build()
        }
      }
	  }
    stage('Docker Build') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { skip } }
          not { expression { return params.PUBLISH } }
        }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (params.RELEASE_BUILD || isHotfix) {
            env.VERSION = (isHotfix) ? env.HOTFIX_NAME : params.RELEASE_NAME
              env.DOCKER_TAG = dockerize.dockerBuildWithSpecificTag(
              tag: params.RELEASE_NAME,
              path: env.DOCKER_PATH,
              artifacts: [sbtbuild.getDockerArtifact()]
            )
          } else {
            env.DOCKER_TAG = dockerize.dockerBuildWithDefaultTag(
              version: 'STAGING',
              sha: env.GIT_COMMIT,
              path: env.DOCKER_PATH,
              artifacts: [sbtbuild.getDockerArtifact()]
            )
          }
        }
      }
      post {
        success {
          script {
            if (isHotfix) {
              env.GIT_TAG = releaseTag.create(env.HOTFIX_NAME)
            } else if (params.RELEASE_BUILD) {
              env.GIT_TAG = releaseTag.getFormattedTag(params.RELEASE_NAME)
              if (releaseTag.doesReleaseTagExist(params.RELEASE_NAME)) {
                echo "REBUILD: Tag ${env.GIT_TAG} already exists"
                return
              }
              if (params.RELEASE_DRY_RUN) {
                echo "DRY RUN: Would have created ${env.GIT_TAG} and pushed it to the repo"
                currentBuild.description = "${service}:${params.RELEASE_NAME} - DRY RUN"
                return
              }
              releaseTag.create(params.RELEASE_NAME)
            }
          }
        }
      }
    }
    stage('Publish Image') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { skip } }
          not { expression { return params.PUBLISH } }
          not { expression { return params.RELEASE_BUILD && params.RELEASE_DRY_RUN } }
        }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (isHotfix || params.RELEASE_BUILD) {
            env.BUILD_ID = dockerize.publish(sourceTag: env.DOCKER_TAG)
          } else {
            env.BUILD_ID = dockerize.publish(
              sourceTag: env.DOCKER_TAG,
              environments: ['internal']
            )
          }
          currentBuild.description = env.BUILD_ID
        }
      }
      post {
        success {
          script {
            if (isHotfix || params.RELEASE_BUILD) {
              env.PURPOSE = (isHotfix) ? 'hotfix' : 'initial'
              env.RELEASE_ID = (isHotfix) ? env.CURRENT_RELEASE_NAME : params.RELEASE_NAME
              Map buildInfo = [
                "project_id": service,
                "build_id": env.BUILD_ID,
                "release_id": env.RELEASE_ID,
                "git_tag": env.GIT_TAG,
                "purpose": env.PURPOSE,
              ]
              createBuild(
                buildInfo,
                rmsSupportedEnvironment.production
              )
            }
          }
        }
      }
    }
    stage('Deploy') {
      when {
        not { expression { isPr } }
        not { expression { skip } }
        not { expression { return params.RELEASE_BUILD } }
        not { expression { return params.PUBLISH } }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          env.ENVIRONMENT = (isHotfix) ? 'rc' : 'staging'
          marathonDeploy(
            serviceName: service,
            tag: env.BUILD_ID,
            environment: env.ENVIRONMENT
          )
        }
      }
      post {
        success {
          script {
            if (isHotfix) {
              Map deployInfo = [
                "build_id": env.BUILD_ID,
                "environment": env.ENVIRONMENT,
              ]
              createDeployment(
                deployInfo,
                rmsSupportedEnvironment.production
              )
            }
          }
        }
      }
    }
  }
  post {
    failure {
      script {
        boolean buildingMain = (env.JOB_NAME.contains("${service}/main"))
        if (buildingMain) {
          teamsMessage(
            message: "[${currentBuild.fullDisplayName}](${env.BUILD_URL}) has failed in stage ${lastStage}",
            webhookCredentialID: WEBHOOK_ID
          )
        }
      }
    }
  }
}
