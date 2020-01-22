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
    And oplog archive is on mongodb01

  Scenario: First backup was done successfully
    Given mongodb01 has test mongodb data test1
    When we create timestamp #10 via mongodb01
    And we create mongodb01 backup
    Then we got 1 backup entries of mongodb01

  Scenario: First database data saved successfully
    When we load mongodb01 with "load/mlc.json" config
    And we create timestamp #0 via mongodb01
    And we save mongodb01 data #0
    Then we got 1 backup entries of mongodb01

  Scenario: Second backup was done successfully
    Given mongodb01 has test mongodb data test2
    When we create timestamp #11 via mongodb01
    When we create mongodb01 backup
    Then we got 2 backup entries of mongodb01

  Scenario: Second database data saved successfully
    When we load mongodb01 with "load/mongo_load_config2.json" config
    And we create timestamp #1 via mongodb01
    And we save mongodb01 data #1
    Then we got 2 backup entries of mongodb01

  Scenario: First "ASD" restored successfully
    Given mongodb02 has no data
    When we restore #1 backup to mongodb02
    And we restore from #10 timestamp to #0 timestamp to mongodb02
    And we save mongodb02 data #2
    Then we have same data in #0 and #2

  Scenario: Second "ASD" restored successfully
    Given mongodb02 has no data
    When we restore #1 backup to mongodb02
    And we restore from #10 timestamp to #1 timestamp to mongodb02
    And we save mongodb02 data #3
    Then we have same data in #1 and #3

  #TODO: Support this scenario
#  Scenario: Third "ASD" restored successfully
#    Given mongodb02 has no data
#    When we restore #0 backup to mongodb02
#    And we wait for 10 seconds
#    And we restore from #11 timestamp to #1 timestamp to mongodb02
#    And we wait for 10 secondsss
#    And we save mongodb02 data #4
#    Then we have same data in #1 and #4
