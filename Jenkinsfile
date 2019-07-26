pipeline {
    agent { label 'vault-token' }

    options {
        timestamps()
        ansiColor('xterm')
        disableConcurrentBuilds()
    }
    environment {
        PATH = "${tool('vault')}:${tool('sbt')}:$PATH"
        // Some wiring is broken between the custom-tools plugin and
        // the pipeline plugin which prevents these vars from being
        // injected when pulling in the custom 'vault' tool.
        VAULT_ADDR = 'https://clotho.broadinstitute.org:8200'
        VAULT_TOKEN_PATH = '/etc/vault-token-monster'
    }
    stages {
        stage('Check formatting') {
            steps {
                sh 'sbt scalafmtCheckAll'
            }
        }
        stage('Compile') {
            steps {
                sh 'sbt Compile/compile Test/compile IntegrationTest/compile'
            }
        }
        stage('Test') {
            steps {
                sh 'sbt "set ThisBuild/coverageEnabled := true" test'
            }
        }
        stage('Integration Test') {
            steps {
                sh 'sbt "set ThisBuild/coverageEnabled := true" IntegrationTest/test'
            }
        }
        stage('Publish') {
            when { branch 'master' }
            steps {
                script {
                    def vaultPath = 'secret/dsp/accts/artifactory/dsdejenkins'
                    def parts = [
                            'set +x;',
                            'echo Publishing to Artifactory...;',
                            'export VAULT_TOKEN=$(cat $VAULT_TOKEN_PATH);',
                            "ARTIFACTORY_USERNAME=\$(vault read -field=username $vaultPath)",
                            "ARTIFACTORY_PASSWORD=\$(vault read -field=password $vaultPath)",
                            'sbt publish'
                    ]
                    sh parts.join(' ')
                }
            }
        }
    }
    post {
        always {
            junit '**/target/test-reports/*'
        }
        success {
            script {
                def vaultPath = 'secret/dsde/monster/dev/codecov/monster-storage-libs'
                def parts = [
                        '#!/bin/bash',
                        'set +x',
                        'echo Publishing code coverage...',
                        'export VAULT_TOKEN=$(cat $VAULT_TOKEN_PATH)',
                        "CODECOV_TOKEN=\$(vault read -field=token $vaultPath)",
                        'sbt coverageAggregate',
                        'bash <(curl -s https://codecov.io/bash) -v -d -t ${CODECOV_TOKEN}'
                ]
                sh parts.join('\n')
            }
        }
        cleanup {
            cleanWs()
        }
    }
}
