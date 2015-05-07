require 'iron_mq'

class MqMigrator
  def initialize(token, host_from, host_to)
    @token = token
    @from =  host_from
    @to = host_to

    if @token.nil? || @token == ""
      puts "Please provide token"
      exit 1
    end
    if @from.nil? || @from == ""
      puts "Please provide source host"
      exit 1
    end
    if @to.nil? || @to == ""
      puts "Please provide destination host"
      exit 1
    end

    @client_from = {}
    @client_to = {}
  end

  def move_queue(project_id, queue_name, options)
    verify_project_id(project_id)

    puts "  Moving queue #{queue_name}"

    info = client_from(project_id).queue(queue_name).info
    info_to = client_to(project_id).queue(queue_name).info rescue nil
    info.delete('size')
    info.delete('total_messages')
    if info_to
      info_to.delete('size')
      info_to.delete('total_messages')
    end

    if info_to.nil?
      puts '    Creating target queue'
    elsif info == info_to
      puts '    Target queue already exists and exactly same as source one. Resuming'
    else
      puts '    Deleting target queue because it\'s different from the source one'

      client_to(project_id).queue(queue_name).delete_queue
    end

    # Create target queue with all possible params
    client_to(project_id).create_queue(queue_name, info)

    if info['type'] == 'pull'
      if options[:skip_messages]
        puts "\n    Skipping messages migration"
      else
        move_messages(project_id, queue_name, options)
      end
    else
      puts "\n    Push queue, skipping messages migration"
      true
    end
  end

  def move_messages(project_id, queue_name, options)
    n = options[:n]
    counter = 0
    begin
      msgs = client_from(project_id).queue(queue_name).get(n: n)
      if msgs.count > 0
        client_to(project_id).queue(queue_name).post(msgs.map{ |msg| {body: msg.body} })
        client_from(project_id).queue(queue_name).delete_messages(msgs.map{ |msg| {id: msg.id, reservation_id: msg.reservation_id} })
        print '.'
      end
      counter += msgs.count
    end while msgs.count >= n

    puts "\n    Migrated #{counter} messages" if counter > 0

    true
  end

  def move_queues(project_id, options)
    verify_project_id(project_id)

    per_page = 100
    queues = []
    previous = nil

    opts = {raw: true, per_page: per_page}
    begin
      begin
        opts.merge!(previous: previous) if previous
        l1 = client_from(project_id).list(opts.merge({}))
        res = l1[0].raw[1].map { |queue| queue['name'] }
        queues += res
        previous = res.last
      end while res.count >= per_page
    rescue StandardError => ex
      puts "[WARN] Can not get list of queues for project #{project_id}: #{ex.message}"
      return
    end

    puts "--Project ##{project_id}".ljust(80, '-')
    puts "  Moving #{queues.count} queues from #{@from} to #{@to}\n"

    queues.each do |queue_name|
      begin
        move_queue(project_id, queue_name, options)
      rescue StandardError => ex
        puts "[WARN] Can not move queue '#{queue_name}': #{ex.message}"
      end
    end
  end

  private

  def verify_project_id(project_id)
    if project_id.nil? || project_id == ""
      puts "project_id is required"
      exit 1
    end
  end

  def client_from(project_id)
    @client_from[project_id] ||= get_client(@from, project_id)
  end

  def client_to(project_id)
    @client_to[project_id] ||= get_client(@to, project_id)
  end

  def get_client(full_host, project_id)
    host, port = full_host.split(':')

    IronMQ::Client.new(
      token: @token,
      project_id: project_id,
      host: host,
      port: port || 80,
      scheme: 'http'
    )
  end
end