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
                            image 'registry.ci.lizardfs.com/lizardfs-ci:pipeline-cpplint-latest'
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
                            image 'registry.ci.lizardfs.com/lizardfs-ci:pipeline-cppcheck-latest'
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
                            image 'registry.ci.lizardfs.com/lizardfs-ci:pipeline-debian11.5-build-latest'
                            registryUrl env.dockerRegistry
                            registryCredentialsId env.dockerRegistrySecretId
                        }
                    }
                    steps {
                        cleanWs()
                        checkout scm
                        cmakeBuild buildDir: 'build/lizardfs', buildType: 'RelWithDebInfo', cleanBuild: true, cmakeArgs: '''-DENABLE_TESTS=YES
                            -DCMAKE_INSTALL_PREFIX=${WORKSPACE}/install/lizardfs/
                            -DENABLE_WERROR=NO
                            -DLIZARDFS_TEST_POINTER_OBFUSCATION=1
                            -DENABLE_CLIENT_LIB=YES
                            -DENABLE_NFS_GANESHA=NO
                            -DENABLE_POLONAISE=NO''', generator: 'Unix Makefiles', installation: 'InSearchPath'
                        sh 'make -C build/lizardfs -j$(nproc) install'
                        stash allowEmpty: true, name: 'compilation-result', includes: "build/lizardfs/**/*"
                        stash allowEmpty: true, name: 'installation-result', includes: "install/lizardfs/**/*"
                    }
                }
                stage('Package') {
                    agent {
                        docker {
                            label 'docker'
                            image 'registry.ci.lizardfs.com/lizardfs-ci:pipeline-debian11.5-build-latest'
                            registryUrl env.dockerRegistry
                            registryCredentialsId env.dockerRegistrySecretId
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
                            image 'registry.ci.lizardfs.com/lizardfs-ci:pipeline-debian11.5-test-latest'
                            registryUrl env.dockerRegistry
                            registryCredentialsId env.dockerRegistrySecretId
                        }
                    }
                    steps {
                        unstash 'compilation-result'
                        sh '''
                            export LIZARDFS_ROOT=${WORKSPACE}/install/lizardfs
                            echo "LIZARDFS_ROOT: ${LIZARDFS_ROOT}"
                            export TEST_OUTPUT_DIR=${WORKSPACE}/test_output
                            echo "TEST_OUTPUT_DIR: ${TEST_OUTPUT_DIR}"
                            export TERM=xterm

                            killall -9 lizardfs-tests || true
                            mkdir -m 777 -p ${TEST_OUTPUT_DIR}
                            rm -rf ${TEST_OUTPUT_DIR:?}/* || true
                            rm -rf /mnt/ramdisk/* || true
                            #lcov --directory ${WORKSPACE}/build/lizardfs/ --capture --output-file ${TEST_OUTPUT_DIR}/code_coverage.info -rc lcov_branch_coverage=1
                            ${WORKSPACE}/build/lizardfs/src/unittests/unittests --gtest_color=yes --gtest_output=xml:${TEST_OUTPUT_DIR}/unit_test_results.xml
                            #genhtml ${TEST_OUTPUT_DIR}/code_coverage.info --branch-coverage --output-directory ${TEST_OUTPUT_DIR}/code_coverage_report/ || true
                            #stash allowEmpty: true, name: 'coverage-report', includes: "${TEST_OUTPUT_DIR}/code_coverage_report/**/*"
                            #archiveArtifacts artifacts: '${TEST_OUTPUT_DIR}/unit_test_results.xml', followSymlinks: false
                        '''
                    }
                }
                stage('Sanity') {
                    agent {
                        docker {
                            label 'docker'
                            image 'registry.ci.lizardfs.com/lizardfs-ci:pipeline-debian11.5-test-latest'
                            registryUrl env.dockerRegistry
                            registryCredentialsId env.dockerRegistrySecretId
                            args  '--cap-add SYS_ADMIN --device=/dev/fuse:/dev/fuse --security-opt="apparmor=unconfined" --tmpfs /mnt/ramdisk:rw,mode=1777,size=2g --ulimit core=-1'
                        }
                    }
                    steps {
                        unstash 'installation-result'
                        sh '''
                            export LIZARDFS_ROOT=${WORKSPACE}/install/lizardfs
                            echo "LIZARDFS_ROOT: ${LIZARDFS_ROOT}"
                            export TEST_OUTPUT_DIR=${WORKSPACE}/test_output
                            echo "TEST_OUTPUT_DIR: ${TEST_OUTPUT_DIR}"
                            export TERM=xterm

                            killall -9 lizardfs-tests || true
                            mkdir -m 777 -p ${TEST_OUTPUT_DIR}
                            rm -rf ${TEST_OUTPUT_DIR:?}/* || true
                            rm -rf /mnt/ramdisk/* || true
                            ${LIZARDFS_ROOT}/bin/lizardfs-tests --gtest_color=yes --gtest_filter='SanityChecks.*' --gtest_output=xml:${TEST_OUTPUT_DIR}/sanity_test_results.xml
                            #archiveArtifacts artifacts: '${TEST_OUTPUT_DIR}/sanity_test_results.xml', followSymlinks: false
                        '''
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
