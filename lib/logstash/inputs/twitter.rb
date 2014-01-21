# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "json"

# Read events from the twitter streaming api.
class LogStash::Inputs::Twitter < LogStash::Inputs::Base

  config_name "twitter"
  milestone 1

  # Your twitter app's consumer key
  #
  # Don't know what this is? You need to create an "application"
  # on twitter, see this url: <https://dev.twitter.com/apps/new>
  config :consumer_key, :validate => :string, :required => true

  # Your twitter app's consumer secret
  #
  # If you don't have one of these, you can create one by
  # registering a new application with twitter:
  # <https://dev.twitter.com/apps/new>
  config :consumer_secret, :validate => :password, :required => true

  # Your oauth token.
  #
  # To get this, login to twitter with whatever account you want,
  # then visit <https://dev.twitter.com/apps>
  #
  # Click on your app (used with the consumer_key and consumer_secret settings)
  # Then at the bottom of the page, click 'Create my access token' which
  # will create an oauth token and secret bound to your account and that
  # application.
  config :oauth_token, :validate => :string, :required => true
  
  # Your oauth token secret.
  #
  # To get this, login to twitter with whatever account you want,
  # then visit <https://dev.twitter.com/apps>
  #
  # Click on your app (used with the consumer_key and consumer_secret settings)
  # Then at the bottom of the page, click 'Create my access token' which
  # will create an oauth token and secret bound to your account and that
  # application.
  config :oauth_token_secret, :validate => :password, :required => true

  # Any keywords to track in the twitter stream
  config :keywords, :validate => :array

  # Any location to track in the twitter stream
  config :location, :validate => :string

  public
  def register
    require "twitter"
    @client = Twitter::Streaming::Client.new do |c|
      c.consumer_key = @consumer_key
      c.consumer_secret = @consumer_secret.value
      c.access_token = @oauth_token
      c.access_token_secret = @oauth_token_secret.value
    end
  end

  public
  def run(queue)
    @logger.info("Starting twitter tracking", :keywords => @keywords, :location => @location)
    filters = {}
    filters[:track] = @keywords.join(",") if @keywords
    filters[:locations] = @location if @location
    @client.filter(filters) do |tweet|
      @logger.info? && @logger.info("Got tweet", :user => tweet.user.screen_name, :text => tweet.text)
      event = LogStash::Event.new(
        "@timestamp" => tweet.created_at.gmtime,
        "message" => tweet.full_text,
        "user" => tweet.user.screen_name,
        "client" => tweet.source,
        "retweeted" => tweet.retweeted?,
        "source" => "http://twitter.com/#{tweet.user.screen_name}/status/#{tweet.id}",
      )
      decorate(event)
      event["in-reply-to"] = tweet.in_reply_to_status_id if tweet.reply?

      # Extract 'entities' into their own field:
      #   media (pictures, video)
      #   urls (links)
      #   hashtags (#)
      #   user_mentions (@)
      {
        media: 'media_url_https',
        urls: 'expanded_url',
        hashtags: 'text',
        user_mentions: 'screen_name'
      }.each do |_entity, key|
        unless (entity = tweet.send(_entity)).empty?
          event[_entity] = entity.map(&key.to_sym).map(&:to_s)
        end
      end

      # For tweets with geocoordinates
      # Add the coords to the event
      # Check that the tweet is actually within the specified bounds,
      # twitter's geo-accuracy isn't great.
      # Only keep events that are inside our @location
      if @location && tweet.geo && !(coords = tweet.geo.coordinates).nil?
        event['latlng'] = coords.to_s
        swlon, swlat, nelon, nelat = @location.split(',').map(&:to_f)
        sw = [swlat, swlon]
        ne = [nelat, nelon]

        lls = sw.zip(coords, ne)
        geofenced = lls.all? {|sw, point, ne| !!point && sw < point && point < ne}
      end

      match = tweet.text.match(/artsmia/) || tweet.user.screen_name.match(/artsmia/) || event[:urls] && event[:urls].any? {|url| url.match(/artsmia/) }
      # Only add the event if it's in our geofence or has our keywords in the full_text
      if geofenced || match
        puts '.'
        queue << event
      end
    end # client.filter
  rescue Interrupt
    return
  end # def run
end # class LogStash::Inputs::Twitter
