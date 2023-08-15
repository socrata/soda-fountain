// Set up the libraries
@Library('socrata-pipeline-library')

// set up variables
def service = 'soda-fountain'
def project_wd = 'soda-fountain-jetty'
def isPr = env.CHANGE_ID != null;

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service, project_wd)
def dockerize = new com.socrata.Dockerize(steps, service, env.BUILD_NUMBER)
def releaseTag = new com.socrata.ReleaseTag(steps, service)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build without creating a new tag.')
    string(name: 'RELEASE_NAME', defaultValue: '', description: 'For release builds, the release name which is used for the git tag and the deploy tag.')
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
    booleanParam(name: 'PUBLISH', defaultValue: false, description: 'Set to true to manually initiate a publish build - you must also specify PUBLISH_SHA')
    string(name: 'PUBLISH_SHA', defaultValue: '', description: 'For publish builds, the git commit SHA or branch to build from')
  }
  agent {
    label params.AGENT
  }
  environment {
    SERVICE = "${service}"
    DOCKER_PATH = './docker'
  }
  stages {
    stage('Release Tag') {
      when {
        expression { return params.RELEASE_BUILD }
      }
      steps {
        script {
          if (params.RELEASE_DRY_RUN) {
            echo 'DRY RUN: Skipping release tag creation'
          }
          else {
            releaseTag.create(params.RELEASE_NAME)
          }
        }
      }
    }
    stage('Build') {
      when {
        not { expression { return params.PUBLISH } }
      }
      steps {
        script {
          // perform any needed modifiers on the build parameters here
          sbtbuild.setNoSubproject(true)
          sbtbuild.setScalaVersion("2.12")
          sbtbuild.setSubprojectName("sodaFountainJetty")
          sbtbuild.setSrcJar("soda-fountain-jetty/target/soda-fountain-jetty-assembly.jar")

          // build
          echo "Building sbt project..."
          sbtbuild.build()
        }
      }
	  }
    stage('Publish') {
      when {
        expression { return params.PUBLISH }
      }
      steps {
        script {
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
    stage('Dockerize') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { return params.PUBLISH } }
        }
      }
      steps {
        script {
          if (params.RELEASE_BUILD) {
            env.REGISTRY_PUSH = (params.RELEASE_DRY_RUN) ? 'none' : 'all'
            env.DOCKER_TAG = dockerize.docker_build_specify_tag_and_push(params.RELEASE_NAME, env.DOCKER_PATH, sbtbuild.getDockerArtifact(), env.REGISTRY_PUSH)
          } else {
            env.REGISTRY_PUSH = 'internal'
            env.DOCKER_TAG = dockerize.docker_build('STAGING', env.GIT_COMMIT, env.DOCKER_PATH, sbtbuild.getDockerArtifact(), env.REGISTRY_PUSH)
          }
          currentBuild.description = env.DOCKER_TAG
        }
      }
      post {
        success {
          script {
            if (params.RELEASE_BUILD && !params.RELEASE_DRY_RUN) {
              echo env.DOCKER_TAG // For now, just print the deploy tag in the console output -- later, communicate to release metadata service
            }
          }
        }
      }
    }
    stage('Deploy') {
      when {
        not { expression { isPr } }
        not { expression { return params.RELEASE_BUILD } }
        not { expression { return params.PUBLISH } }
      }
      steps {
        script {
          // uses env.SERVICE and env.DOCKER_TAG, deploys to staging by default
          marathonDeploy()
        }
      }
    }
  }
}
