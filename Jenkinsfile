def imageTags = [:]
pipeline {
    agent {
        label 'docker'
    }
    environment{
        projectName = 'lizardfs.ci'
        registryPrefix = "registry.ci.lizardfs.com"
        dockerRegistry = "https://${registryPrefix}"
        dockerRegistrySecretId = 'private-docker-registry-credentials'
        imageName = projectName.replaceAll('\\.','-').replaceAll('/','-')
        jenkinsUsername = "${sh(script:'id -un', returnStdout: true).trim()}"
        jenkinsUserId = "${sh(script:'id -u', returnStdout: true).trim()}"
        jenkinsGroupId = "${sh(script:'id -g', returnStdout: true).trim()}"
    }
    options {
        skipDefaultCheckout()
        timestamps()
        ansiColor("xterm")
        parallelsAlwaysFailFast()
        preserveStashes(buildCount: 2)
    }
    stages {
        stage('Prepare') {
            steps {
                script {
                    def branchedStages = [:]
                    stageNames=['cppcheck', 'cpplint', 'bookworm-build', 'bookworm-test']
                    stageNames.each { stageName ->
                        branchedStages["${stageName}"] = {
                            stage("${stageName}") {
                                node('docker') {
                                    cleanWs()
                                    checkout scm
                                    def commitId = getCommitId()
                                    def containerTag = getPartialTag(env.BRANCH_NAME, stageName, commitId)
                                    def latestTag = getPartialTag(env.BRANCH_NAME, stageName)
                                    def imagePrefix="${registryPrefix}/${imageName}"
                                    println("${imagePrefix}:${containerTag}")
                                    println("${imagePrefix}:${latestTag}")
                                    docker.withRegistry(env.dockerRegistry, env.dockerRegistrySecretId) {
                                        dockerTryPull("${imagePrefix}:${latestTag}")
                                        def ciImage = docker.build("${imagePrefix}:${containerTag}", """\
                                            --build-arg GROUP_ID=${jenkinsGroupId} \
                                            --build-arg USER_ID=${jenkinsUserId} \
                                            --build-arg USERNAME=${jenkinsUsername} \
                                            --cache-from ${imagePrefix}:${latestTag} \
                                            --file tests/ci_build/Dockerfile.${stageName} .
                                        """)
                                        ciImage.push(containerTag)
                                        ciImage.push(latestTag)
                                        imageTags[stageName] = containerTag
                                    }
                                }
                            }
                        }
                    }
                    parallel branchedStages
                }
            }
        }
        stage('Process') {
            parallel {
                stage('Lint') {
                    agent {
                        docker {
                            label 'docker'
                            image 'registry.ci.lizardfs.com/lizardfs-ci:' + imageTags['cpplint']
                            registryUrl env.dockerRegistry
                            registryCredentialsId env.dockerRegistrySecretId
                        }
                    }
                    steps {
                        cleanWs()
                        checkout scm
                        sh 'cpplint --counting=detailed --linelength=120 --output=vs7 src/**/*.h src/**/*.cc 2> cpplint.log || true'
                        archiveArtifacts artifacts: 'cpplint.log', followSymlinks: false
                        stash allowEmpty: true, name: 'cpplint-result', includes: "cpplint.log"

                    }
                }
                stage('Check') {
                    agent {
                        docker {
                            label 'docker'
                            image 'registry.ci.lizardfs.com/lizardfs-ci:' + imageTags['cppcheck']
                            registryUrl env.dockerRegistry
                            registryCredentialsId env.dockerRegistrySecretId
                        }
                    }
                    steps {
                        cleanWs()
                        checkout scm
                        sh 'cppcheck --enable=all --inconclusive --xml --xml-version=2 ./src 2> cppcheck.xml'
                        archiveArtifacts artifacts: 'cppcheck.xml', followSymlinks: false
                        stash allowEmpty: true, name: 'cppcheck-result', includes: "cppcheck.xml"

                    }
                }
                stage('Build') {
                    agent {
                        docker {
                            label 'docker'
                            image 'registry.ci.lizardfs.com/lizardfs-ci:' + imageTags['bookworm-build']
                            registryUrl env.dockerRegistry
                            registryCredentialsId env.dockerRegistrySecretId
                            args  '--security-opt seccomp=unconfined'
                        }
                    }
                    steps {
                        cleanWs()
                        checkout scm
                        sh 'tests/ci_build/run-build.sh'
                        stash allowEmpty: true, name: 'compilation-result', includes: "build/lizardfs/**/*"
                        stash allowEmpty: true, name: 'installation-result', includes: "install/lizardfs/**/*"
                    }
                }
                stage('Package') {
                    agent {
                        docker {
                            label 'docker'
                            image 'registry.ci.lizardfs.com/lizardfs-ci:' + imageTags['bookworm-build']
                            registryUrl env.dockerRegistry
                            registryCredentialsId env.dockerRegistrySecretId
                            args  '--security-opt seccomp=unconfined'
                        }
                    }
                    steps {
                        cleanWs()
                        checkout scm
                        script {
                            sh "./package.sh"
                            archiveArtifacts artifacts: '*bundle*.tar', followSymlinks: false
                        }
                    }
                }
            }
            post {
                always {
                    unstash 'cppcheck-result'
                    unstash 'cpplint-result'
                    recordIssues enabledForFailure: true, tool: cppCheck(name: "Lint: cppcheck", pattern: 'cppcheck.xml')
                    recordIssues enabledForFailure: true, tool: cppLint(name: "Lint: cpplint", pattern: 'cpplint.log')
                }
            }
        }
        stage('Test') {
            parallel {
                stage('Unit') {
                    agent {
                        docker {
                            label 'docker'
                            image 'registry.ci.lizardfs.com/lizardfs-ci:' + imageTags['bookworm-test']
                            registryUrl env.dockerRegistry
                            registryCredentialsId env.dockerRegistrySecretId
                        }
                    }
                    steps {
                        unstash 'compilation-result'
                        sh 'tests/ci_build/run-tests-unit.sh'
                    }
                }
                stage('Sanity') {
                    agent {
                        docker {
                            label 'docker'
                            image 'registry.ci.lizardfs.com/lizardfs-ci:' + imageTags['bookworm-test']
                            registryUrl env.dockerRegistry
                            registryCredentialsId env.dockerRegistrySecretId
                            args  '--security-opt seccomp=unconfined --cap-add SYS_ADMIN --device=/dev/fuse:/dev/fuse --security-opt="apparmor=unconfined" --tmpfs /mnt/ramdisk:rw,mode=1777,size=2g --ulimit core=-1'
                        }
                    }
                    steps {
                        unstash 'installation-result'
                        sh 'tests/ci_build/run-tests-sanitycheck.sh'
                    }
                }
            }
        }
    }
    post {
        always {
            cleanWs()
        }
    }
}

def getVersion() {
    "0.1.0"
}

def getBuildTimestamp() {
    return sh(returnStdout: true, script: 'date -u +"%Y%m%d-%H%M%S"').trim()
}

def getCommitId() {
    return sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
}

def getPartialTag(branchName, customTag, commitId = 'latest') {
    return "${branchName}-${customTag}-${commitId}"
}

def getTag(version, branchName, commitId = 'HEAD') {
    def branchStatus = (branchName == 'main' ) ? 'stable' : 'unstable'
    def timestamp = getBuildTimestamp()
    return "${version}-${timestamp}-${branchStatus}-${branchName}-${commitId}"
}

def dockerTryPull(image) {
    try {
        docker.image(image).pull()
    }
    catch (Exception ex) {
        println("Catching failure pulling ${image} image")
    }
}
