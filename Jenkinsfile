pipeline {
    agent any

    parameters {
        choice(
            name: 'LOAD_TOOL',
            choices: ['SQOOP', 'SPARK'],
            description: 'Choose Sqoop or Spark'
        )

        choice(
            name: 'LOAD_TYPE',
            choices: ['FULL', 'INCREMENTAL'],
            description: 'Choose full or incremental load'
        )

        choice(
            name: 'LOAD_SCOPE',
            choices: ['ALL', 'DIMENSIONS_ONLY', 'FACT_ONLY', 'SINGLE_TABLE'],
            description: 'Used mainly for Sqoop table loading'
        )

        string(
            name: 'TABLE_NAME',
            defaultValue: '',
            description: 'Required only when LOAD_SCOPE = SINGLE_TABLE. Example: dim_date'
        )
    }

    environment {
    REMOTE_HOST = '13.41.167.97'
    REMOTE_USER = 'consultant'
    REMOTE_PASSWORD = 'Cl0ud3ra@2026#Secur3!'

    PROJECT_DIR = '/home/consultant/hiren/TFL_Project_1'
    HDFS_RAW_BASE = '/tmp/tfl_project_hadoop'
    HDFS_GOLD_BASE = '/tmp/tfl_project_hadoop/gold'

    SSH_OPTS = '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'

    SQOOP_FULL_SCRIPT = 'ON_PREM/data_ingestion_batch/src/raw_layer/full_load/raw_sqoop_full_load.sh'
    SQOOP_INCREMENTAL_SCRIPT = 'ON_PREM/data_ingestion_batch/src/raw_layer/incremental_load/raw_incremental_load.sh'

    SPARK_FULL_SCRIPT = 'ON_PREM/data_ingestion_batch/src/raw_layer/full_load/spark/tfl_spark_analysis.py'
    SPARK_INCREMENTAL_SCRIPT = 'ON_PREM/data_ingestion_batch/src/raw_layer/incremental_load/spark/incremental.py'
}

    stages {

        stage('Validate Parameters') {
            steps {
                script {
                    if (params.LOAD_SCOPE == 'SINGLE_TABLE' && params.TABLE_NAME.trim() == '') {
                        error "TABLE_NAME is required when LOAD_SCOPE is SINGLE_TABLE"
                    }

                    echo "LOAD_TOOL      = ${params.LOAD_TOOL}"
                    echo "LOAD_TYPE      = ${params.LOAD_TYPE}"
                    echo "LOAD_SCOPE     = ${params.LOAD_SCOPE}"
                    echo "TABLE_NAME     = ${params.TABLE_NAME}"
                    echo "REMOTE_HOST    = ${env.REMOTE_HOST}"
                    echo "PROJECT_DIR    = ${env.PROJECT_DIR}"
                    echo "HDFS_RAW_BASE  = ${env.HDFS_RAW_BASE}"
                    echo "HDFS_GOLD_BASE = ${env.HDFS_GOLD_BASE}"
                }
            }
        }
        stage('Test SSH Login') {
                    steps {
                echo '========================================='
                echo 'Testing SSH Login to Cloudera'
                echo '========================================='

                sh '''
                    set +x

                    echo "Remote user: ${REMOTE_USER}"
                    echo "Remote host: ${REMOTE_HOST}"
                    echo "Password length: ${#REMOTE_PASSWORD}"

                    sshpass -p "${REMOTE_PASSWORD}" ssh \
                        -o StrictHostKeyChecking=no \
                        -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "echo CONNECTED_TO_REMOTE && whoami && hostname"
                '''
            }
        }
        stage('Prepare Remote Directory') {
            steps {
        echo '========================================='
        echo 'Stage 2: Create Directories on Cloudera'
        echo '========================================='

        sh '''
            set +x

            sshpass -p "${REMOTE_PASSWORD}" ssh \
                -o StrictHostKeyChecking=no \
                -o UserKnownHostsFile=/dev/null \
                ${REMOTE_USER}@${REMOTE_HOST} \
                "mkdir -p ${PROJECT_DIR}/ON_PREM/data_ingestion_batch/src/raw_layer/full_load \
                          ${PROJECT_DIR}/ON_PREM/data_ingestion_batch/src/raw_layer/full_load/spark \
                          ${PROJECT_DIR}/ON_PREM/data_ingestion_batch/src/raw_layer/incremental_load \
                          ${PROJECT_DIR}/ON_PREM/data_ingestion_batch/src/raw_layer/incremental_load/spark \
                 && echo REMOTE_DIR_READY"
        '''
        }
}

        stage('Copy Scripts to Remote') {
            steps {
                    echo '========================================='
                    echo 'Stage 3: Copy Scripts to Cloudera'
                    echo '========================================='

                    sh '''
                        set +x

                        echo "Copying ON_PREM folder to remote host..."

                        sshpass -p "${REMOTE_PASSWORD}" scp \
                            -o StrictHostKeyChecking=no \
                            -o UserKnownHostsFile=/dev/null \
                            -r ON_PREM \
                            ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/

                        echo "Copying Jenkinsfile to remote host..."

                        sshpass -p "${REMOTE_PASSWORD}" scp \
                            -o StrictHostKeyChecking=no \
                            -o UserKnownHostsFile=/dev/null \
                            Jenkinsfile \
                            ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/ || true

                        echo "Scripts copied to remote host"
                    '''
        }
        }

        stage('Check Remote Tools') {
            steps {
                echo '========================================='
                echo 'Stage 4: Check Remote Tools'
                echo '========================================='

                sh '''
                    set +x

                    sshpass -p "${REMOTE_PASSWORD}" ssh \
                        -o StrictHostKeyChecking=no \
                        -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "
                            echo USER_ON_REMOTE=$(whoami)
                            echo HOSTNAME=$(hostname)
                            echo PATH=$PATH

                            echo Checking Hadoop...
                            which hdfs || true
                            hdfs version || true

                            echo Checking Sqoop...
                            which sqoop || true
                            sqoop version || true

                            echo Checking Spark...
                            which spark-submit || true
                            spark-submit --version || true
                        "
                '''
            }
}
        stage('Select Sqoop Tables') {
            when {
                expression {
                    return params.LOAD_TOOL == 'SQOOP'
                }
            }

            steps {
                script {
                    def dimensions = [
                        'dim_date',
                        'dim_lines',
                        'dim_networks',
                        'dim_stations'
                    ]

                    def factTables = [
                        'fact_passenger_entry_exit',
                        'fact_station_lines'
                    ]

                    if (params.LOAD_SCOPE == 'ALL') {
                        env.TABLE_LIST = (dimensions + factTables).join(',')
                    } else if (params.LOAD_SCOPE == 'DIMENSIONS_ONLY') {
                        env.TABLE_LIST = dimensions.join(',')
                    } else if (params.LOAD_SCOPE == 'FACT_ONLY') {
                        env.TABLE_LIST = factTables.join(',')
                    } else if (params.LOAD_SCOPE == 'SINGLE_TABLE') {
                        env.TABLE_LIST = params.TABLE_NAME.trim()
                    }

                    echo "Sqoop tables selected: ${env.TABLE_LIST}"
                }
            }
        }

stage('Run Sqoop Load on Remote') {
    when {
        expression {
            return params.LOAD_TOOL == 'SQOOP'
        }
    }

    steps {
        script {
            def tables = env.TABLE_LIST.split(',')

            for (table in tables) {
                table = table.trim()

                def targetSuffix = ''
                if (params.LOAD_TYPE == 'FULL') {
                    targetSuffix = '_full_load'
                } else {
                    targetSuffix = '_inc_load'
                }

                def hdfsTargetPath = "${env.HDFS_RAW_BASE}/${table}${targetSuffix}"

                echo "=================================================="
                echo "Running Sqoop ${params.LOAD_TYPE} load on remote"
                echo "Table           : ${table}"
                echo "HDFS target path: ${hdfsTargetPath}"
                echo "=================================================="

                if (params.LOAD_TYPE == 'FULL') {
                    sh """
                        set +x

                        sshpass -p "\${REMOTE_PASSWORD}" ssh \${SSH_OPTS} \${REMOTE_USER}@\${REMOTE_HOST} "
                            cd \${PROJECT_DIR}
                            chmod +x \${SQOOP_FULL_SCRIPT}
                            \${SQOOP_FULL_SCRIPT} ${table} ${hdfsTargetPath}
                        "
                    """
                }

                if (params.LOAD_TYPE == 'INCREMENTAL') {
                    sh """
                        set +x

                        sshpass -p "\${REMOTE_PASSWORD}" ssh \${SSH_OPTS} \${REMOTE_USER}@\${REMOTE_HOST} "
                            cd \${PROJECT_DIR}
                            chmod +x \${SQOOP_INCREMENTAL_SCRIPT}
                            \${SQOOP_INCREMENTAL_SCRIPT} ${table} ${hdfsTargetPath}
                        "
                    """
                }
            }
        }
    }
}
        stage('Run Spark Full Flow on Remote') {
    when {
        expression {
            return params.LOAD_TOOL == 'SPARK' && params.LOAD_TYPE == 'FULL'
        }
    }

    steps {
        sh '''
            set +x

            sshpass -p "${REMOTE_PASSWORD}" ssh \
                -o StrictHostKeyChecking=no \
                -o UserKnownHostsFile=/dev/null \
                ${REMOTE_USER}@${REMOTE_HOST} \
                "
                    cd ${PROJECT_DIR}
                    spark-submit --master local[*] ${SPARK_FULL_SCRIPT}
                "
        '''
    }
}

        stage('Run Spark Incremental Flow on Remote') {
    when {
        expression {
            return params.LOAD_TOOL == 'SPARK' && params.LOAD_TYPE == 'INCREMENTAL'
        }
    }

    steps {
        sh '''
            set +x

            sshpass -p "${REMOTE_PASSWORD}" ssh \
                -o StrictHostKeyChecking=no \
                -o UserKnownHostsFile=/dev/null \
                ${REMOTE_USER}@${REMOTE_HOST} \
                "
                    cd ${PROJECT_DIR}
                    spark-submit --master "local[*]" ${SPARK_INCREMENTAL_SCRIPT}
                "
        '''
    }
}

        stage('Validate HDFS Output on Remote') {
    steps {
        sh '''
            set +x

            if [ "$LOAD_TOOL" = "SQOOP" ]; then
                sshpass -p "${REMOTE_PASSWORD}" ssh \
                    -o StrictHostKeyChecking=no \
                    -o UserKnownHostsFile=/dev/null \
                    ${REMOTE_USER}@${REMOTE_HOST} \
                    "hdfs dfs -ls ${HDFS_RAW_BASE} || true"
            fi

            if [ "$LOAD_TOOL" = "SPARK" ]; then
                sshpass -p "${REMOTE_PASSWORD}" ssh \
                    -o StrictHostKeyChecking=no \
                    -o UserKnownHostsFile=/dev/null \
                    ${REMOTE_USER}@${REMOTE_HOST} \
                    "hdfs dfs -ls ${HDFS_GOLD_BASE} || true"
            fi
        '''
    }
}
    }

    post {
        success {
            echo "Pipeline completed successfully."
        }

        failure {
            echo "Pipeline failed. Check Jenkins console logs."
        }
    }
}