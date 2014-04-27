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

    def identifier
      # support 1.x versions of redis-rb
      if @redis.respond_to?(:server)
        @redis.server
      elsif @redis.respond_to?(:nodes) # distributed
        @redis.nodes.map { |n| n.id }.join(', ')
      else
        @redis.client.id
      end
    end

    # Push something to a queue
    def push_to_queue(queue,encoded_item)
      @redis.pipelined do
        watch_queue(queue)
        @redis.rpush redis_key_for_queue(queue), encoded_item
      end
    end

    # Pop whatever is on queue
    def pop_from_queue(queue)
      @redis.lpop(redis_key_for_queue(queue))
    end

    # Get the number of items in the queue
    def queue_size(queue)
      @redis.llen(redis_key_for_queue(queue)).to_i
    end

    # Examine items in the queue.
    #
    # NOTE: if count is 1, you will get back an object, otherwise you will
    #       get an Array.  I'm not making this up.
    def peek_in_queue(queue, start = 0, count = 1)
      list_range(redis_key_for_queue(queue), start, count)
    end

    def queue_names
      Array(@redis.smembers(:queues))
    end

    def remove_queue(queue)
      @redis.pipelined do
        @redis.srem(:queues, queue.to_s)
        @redis.del(redis_key_for_queue(queue))
      end
    end

    def add_failed_queue(failed_queue_name)
      @redis.sadd(:failed_queues, failed_queue_name)
    end

    def everything_in_queue(queue)
      @redis.lrange(redis_key_for_queue(queue), 0, -1)
    end

    # Remove data from the queue, if it's there, returning the number of removed elements
    def remove_from_queue(queue,data)
      @redis.lrem(redis_key_for_queue(queue), 0, data)
    end

    # Private: do not call
    def watch_queue(queue)
      @redis.sadd(:queues, queue.to_s)
    end

    def num_failed
      @redis.llen(:failed).to_i
    end

    # Returns an array of all known Resque keys in Redis. Redis' KEYS operation
    # is O(N) for the keyspace, so be careful - this can be slow for big databases.
    def all_resque_keys
      @redis.keys("*").map do |key|
        key.sub("#{redis.namespace}:", '')
      end
    end


    # Private: do not call
    def list_range(key, start = 0, count = 1)
      if count == 1
        @redis.lindex(key, start)
      else
        Array(@redis.lrange(key, start, start+count-1))
      end
    end

  private
    
    def redis_key_for_queue(queue)
      "queue:#{queue}"
    end


  end
end
