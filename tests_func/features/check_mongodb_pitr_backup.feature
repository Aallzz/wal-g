Feature: MongoDB PITR backups check

  Background: Wait for working infrastructure
    Given a working mongodb on mongodb01
    And a working mongodb on mongodb02
    And a configured s3 on minio01
    And mongodb replset initialized on mongodb01
    And mongodb replset initialized on mongodb02
    And mongodb auth initialized on mongodb01
    And mongodb auth initialized on mongodb02
    And mongodb role is primary on mongodb01
    And mongodb role is primary on mongodb02

  Scenario: First backup was done successfully
    Given mongodb01 has test mongodb data test1
    When we create mongodb01 backup
    Then we got 1 backup entries of mongodb01

  Scenario: Loading database
    When we load mongodb01 with "mongo_load_config.json" config
    And we create timestamp #0 via mongodb01


