// MongoDB Test Data Loader - Generated 2025-11-26T23:53:27.340Z
// Usage: mongosh < this_file.js

use test_db;

// string_tests
db.string_tests.drop();
db.string_tests.insertMany([
  {
    "_id": 1,
    "str": "hello",
    "str2": "world",
    "empty": "",
    "mixed": "HeLLo WoRLD"
  },
  {
    "_id": 2,
    "str": "UPPERCASE",
    "str2": "lowercase",
    "empty": "",
    "mixed": "123abc"
  },
  {
    "_id": 3,
    "str": "  trimme  ",
    "str2": "  spaces  ",
    "empty": null,
    "mixed": "MiXeD"
  },
  {
    "_id": 4,
    "str": "abcdefghij",
    "str2": "klmnop",
    "num": 12345
  },
  {
    "_id": 5,
    "str": "",
    "str2": "",
    "empty": ""
  }
]);

// array_tests
db.array_tests.drop();
db.array_tests.insertMany([
  {
    "_id": 1,
    "arr": [
      1,
      2,
      3,
      4,
      5
    ],
    "tags": [
      "a",
      "b",
      "c"
    ]
  },
  {
    "_id": 2,
    "arr": [
      "x",
      "y",
      "z"
    ],
    "tags": [
      "single"
    ]
  },
  {
    "_id": 3,
    "arr": [],
    "tags": []
  },
  {
    "_id": 4,
    "arr": [
      10
    ],
    "tags": [
      "one",
      "two",
      "three",
      "four"
    ]
  },
  {
    "_id": 5,
    "arr": [
      [
        1,
        2
      ],
      [
        3,
        4
      ]
    ],
    "tags": [
      "nested"
    ]
  },
  {
    "_id": 6,
    "arr": [
      null,
      1,
      null,
      2
    ],
    "tags": [
      "with",
      "nulls"
    ]
  }
]);

// cond_tests
db.cond_tests.drop();
db.cond_tests.insertMany([
  {
    "_id": 1,
    "score": 95,
    "status": "active",
    "value": 100
  },
  {
    "_id": 2,
    "score": 75,
    "status": "active",
    "value": 50
  },
  {
    "_id": 3,
    "score": 55,
    "status": "inactive",
    "value": null
  },
  {
    "_id": 4,
    "score": 45,
    "status": "active",
    "value": 0
  },
  {
    "_id": 5,
    "score": 85,
    "status": "pending",
    "value": -10
  }
]);

// date_tests
db.date_tests.drop();
db.date_tests.insertMany([
  {
    "_id": 1,
    "date": {
      ISODate("2024-01-15T10:30:45.123Z")
    },
    "name": "January morning"
  },
  {
    "_id": 2,
    "date": {
      ISODate("2024-06-21T14:00:00.000Z")
    },
    "name": "June afternoon"
  },
  {
    "_id": 3,
    "date": {
      ISODate("2024-12-25T00:00:00.000Z")
    },
    "name": "Christmas midnight"
  },
  {
    "_id": 4,
    "date": {
      ISODate("2024-02-29T23:59:59.999Z")
    },
    "name": "Leap day end"
  },
  {
    "_id": 5,
    "date": {
      ISODate("2024-07-04T12:00:00.000Z")
    },
    "name": "July 4th noon"
  }
]);

// arith_tests
db.arith_tests.drop();
db.arith_tests.insertMany([
  {
    "_id": 1,
    "a": 10,
    "b": 3
  },
  {
    "_id": 2,
    "a": 0,
    "b": 5
  },
  {
    "_id": 3,
    "a": -10,
    "b": 3
  },
  {
    "_id": 4,
    "a": 100,
    "b": 0
  },
  {
    "_id": 5,
    "a": 7.5,
    "b": 2.5
  },
  {
    "_id": 6,
    "a": null,
    "b": 5
  }
]);

// acc_tests
db.acc_tests.drop();
db.acc_tests.insertMany([
  {
    "_id": 1,
    "group": "A",
    "value": 10,
    "status": "active"
  },
  {
    "_id": 2,
    "group": "A",
    "value": 20,
    "status": "active"
  },
  {
    "_id": 3,
    "group": "A",
    "value": 30,
    "status": "inactive"
  },
  {
    "_id": 4,
    "group": "B",
    "value": 5,
    "status": "active"
  },
  {
    "_id": 5,
    "group": "B",
    "value": null,
    "status": "active"
  },
  {
    "_id": 6,
    "group": "C",
    "value": 100,
    "status": "pending"
  }
]);
