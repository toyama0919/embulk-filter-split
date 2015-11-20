Embulk::JavaPlugin.register_filter(
  "split", "org.embulk.filter.split.SplitFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
