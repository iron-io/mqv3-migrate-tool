#!/usr/bin/ruby

require 'rubygems'
require 'bundler/setup'

require 'iron_mq'
require 'optparse'
require 'json'

options = {}

OptionParser.new do |opts|
  opts.banner = "Usage: move-queues.rb [options]"
  opts.on("--from HOST", "Set source host:port (required)") do |v|
    options[:from] = v
  end
  opts.on("--to HOST", "Set destination host:port (required)") do |v|
    options[:to] = v
  end
  opts.on("-t TOKEN", "--token TOKEN", "Set API token (required)") do |v|
    options[:token] = v
  end
  opts.on("-p PROJECT_ID", "--project-id PROJECT_ID", "Set Project ID") do |v|
    options[:project_id] = v
  end

  opts.on("-q QUEUE_NAME", "--queue QUEUE_NAME", "If set, move only this queue") do |v|
    options[:queue] = v
  end

 	opts.separator ""
  opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
    options[:verbose] = v
  end
  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end
end.parse!

jj options

options[:from] || raise("source host (--from) is required")
options[:to] || raise("detination host (--to) is required")
options[:project_id] || raise("project_id (-p) is required ")
options[:token] || raise("token (-t) is required ")

if options[:verbose]
	IronCore::Logger.logger.level = Logger::DEBUG
	RestClient.log = STDOUT
end


def get_client(options, full_host)
	host, port = full_host.split(':')

	IronMQ::Client.new(
	  token: options[:token],
	  project_id: options[:project_id],
	  host: host,
	  port: port || 80,
	  scheme: 'http'
	)
end

def client_from(options)
	@client_from ||= get_client(options, options[:from])
end

def client_to(options)
	@client_to ||= get_client(options, options[:to])
end

def move_queue(options, queue_name)
	puts "--moving queue #{queue_name}".ljust(80, '-')

	info = client_from(options).queue(queue_name).info

	# Delete target queue if existing
	client_to(options).queue(queue_name).delete_queue

	# Create target queue with all possible params
	client_to(options).create_queue(queue_name, info)

	if info['type'] == 'pull'
		move_messages(options, queue_name)
	else
		true
	end
end

def move_messages(options, queue_name)
	n = 100
	counter = 0
	begin
		msgs = client_from(options).queue(queue_name).get(n: n)
		if msgs.count > 0
			client_to(options).queue(queue_name).post(msgs.map{ |msg| {body: msg.body} })
			client_from(options).queue(queue_name).delete_messages(msgs.map{ |msg| {id: msg.id, reservation_id: msg.reservation_id} })
			print '.'
		end
		counter += msgs.count
	end while msgs.count >= n

	puts "\nMigrated #{counter} messages"

	true
end

# previous: previousQueue, per_page: $scope.perPage, prefix: $scope.prefix
def move_queues(options)
	per_page = 100
	queues = []
	previous = nil

	opts = {raw: true, per_page: per_page}
	begin
		opts.merge!(previous: previous) if previous
		l1 = client_from(options).list(opts.merge({}))
		res = l1[0].raw[1].map { |queue| queue['name'] }
		queues += res
		previous = res.last
	end while res.count >= per_page

	puts "Found #{queues.count} queues"

	queues.each do |queue_name|
		move_queue(options, queue_name)
	end
end



if options[:queue]
	move_queue(options, options[:queue])
else
	move_queues(options)
end
