Embulk::JavaPlugin.register_parser(
  "java_parser_sample", "org.embulk.parser.Java_parser_sampleParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
