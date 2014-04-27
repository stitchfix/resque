module Resque
  # An interface between Resque's persistence and the actual
  # implementation. 
  class DataStore
    def initialize(redis)
      @redis = redis
    end

    def method_missing(sym,*args,&block)
      @redis.send(sym,*args,&block)
    end
  end
end
