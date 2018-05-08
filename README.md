# pac-aurora-synchroniser
A CLI tool to synchronise the latest PAC data between two Aurora instances

[![Circle CI](https://circleci.com/gh/Financial-Times/pac-aurora-synchroniser/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/pac-aurora-synchroniser/tree/master)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/pac-aurora-synchroniser)](https://goreportcard.com/report/github.com/Financial-Times/pac-aurora-synchroniser)[![Coverage Status](https://coveralls.io/repos/github/Financial-Times/pac-aurora-synchroniser/badge.svg)](https://coveralls.io/github/Financial-Times/pac-aurora-synchroniser)

## Introduction

This CLI tool aims to synchronize two AWS aurora instances used for PAC.
The software moves all the records that are more recent in a source database to a target database 
based on the `last_modified` field of each table. 
Tables without such field will be ignored by the synchronisation process.  

## Installation

Download the source code, dependencies and test dependencies:

    curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
    mkdir $GOPATH/src/github.com/Financial-Times/public-things-api
    cd $GOPATH/src/github.com/Financial-Times
    git clone https://github.com/Financial-Times/public-things-api.git
    cd public-things-api && dep ensure -vendor-only
    go build .

## Running locally

1. Run the tests and install the binary:

        go test ./...
        go install

2. Run the binary (using the `help` flag to see the available optional arguments):

        $GOPATH/bin/pac-aurora-synchroniser [--help]

        Options:

        --source-db-connection-url   Connection URL to the database with the most recent data (env $SOURCE_DB_CONNECTION_URL)
        --target-db-connection-url   Connection URL to the database that needs to receive the latest data (env $TARGET_DB_CONNECTION_URL)
        
