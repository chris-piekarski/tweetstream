##
# Two Queues
#   tweets_to_process
#   tweets_to_scrape
#
##
require 'resque'
require 'resque/plugins/workers/lock'
require 'twitter'

module TweetStream

  # added from on_limit callback
  class ScrapeMissedTweets
    extend Resque::Plugins::Workers::Lock

    @queue = :tweets_to_scrape

    #has to accept all perform paramaters & return what should be the lock key
    def self.lock_workers(resque_process_class, missed, tags, since_id, max_id, consumer_key, consumer_secret, oauth_token, oauth_secret)
      return oauth_token
    end

    def self.perform(resque_process_class, missed, tags, since_id, max_id, consumer_key, consumer_secret, oauth_token, oauth_secret)
      #Scrape Tweets
      configure_twitter(consumer_key, consumer_secret, oauth_token, oauth_secret)
      q=tags.join(' OR ')
      Twitter.search(q, :count=>100, :since_id=>since_id, :max_id=>max_id, :include_entities=>true).results.map do |status|
        #puts "#{status.id}: #{status.text}"
        Resque.enqueue(Kernel.const_get(resque_process_class), status.attrs)
      end
      #if we scrape more than once per 5 seconds we will max out our 15 minute slot
      sleep(can_run_every)
    end

    def self.can_run_every
      #api search limits
      ((60*15) / 180).to_int
    end

    def self.configure_twitter(consumer_key, consumer_secret, oauth_token, oauth_secret)
      Twitter.configure do |config|
        config.consumer_key = consumer_key
        config.consumer_secret = consumer_secret
        config.oauth_token = oauth_token
        config.oauth_token_secret = oauth_secret
      end
    end
  end
end
