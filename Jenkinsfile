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
                sh 'sbt test'
            }
        }
        stage('Integration Test') {
            steps {
                sh 'sbt IntegrationTest/test'
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
        cleanup {
            cleanWs()
        }
    }
}
