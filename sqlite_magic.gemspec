# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sqlite_magic/version'

Gem::Specification.new do |spec|
  spec.name          = "sqlite_magic"
  spec.version       = SqliteMagic::VERSION
  spec.authors       = ["Chris Taggart"]
  spec.email         = ["info@opencorporates.com"]
  spec.description   = %q{Sprinkles some magic onto Sqlite3 gem. Sort of extracted from Scraperwiki gem}
  spec.summary       = %q{Sprinkles some magic onto Sqlite3 gem}
  spec.homepage      = "https://github.com/openc/sqlite_magic"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "sqlite3"

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"

end
