# SqliteMagic
[![Build Status](https://travis-ci.org/openc/sqlite_magic.png)](https://travis-ci.org/openc/sqlite_magic)
Experimental abstraction and refactoring of sqlite utility methods from
scraperwiki-ruby gem
Note: Not all functionality has yet been duplicated, and this may not work for
you. Developed and tested on Ruby 1.9.3

## Installation

Add this line to your application's Gemfile:

    gem 'sqlite_magic', :git => 'git@github.com:openc/sqlite_magic.git'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install sqlite_magic

## Usage

TODO: Write usage instructions here

## Changes
* Allow options to be passed when initializing connection (which will be passed to Sqlite)
* insert\_or\_update now defers to #save\_data when hitting error (which caused a problem when missing field)
* busy_timeout can now be set in options

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
