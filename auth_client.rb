require 'rest-client'

class AuthClient
  def initialize(migrator, token, auth_host, options = {})
    @migrator = migrator
    @token = token
    @host = auth_host
    @options = options

    if @token.nil? || @token == ""
      puts "Please provide token"
      exit 1
    end
    if @host.nil? || @host == ""
      puts "Please provide ironauth host"
      exit 1
    end
  end

  def migrate_user(user_id)
    if user_id.nil? || user_id == ""
      puts "Please provide user id"
      exit 1
    end
    migrate_user_projects(get_user(user_id))
  end

  def migrate_all
    users = list_users
    puts "  Found #{users.count} users\n"
    jj users if @options[:verbose]

    users.each do |user|
      migrate_user_projects(user)
    end
  end

  def migrate_user_projects(user)
    puts "\n--Migrating user ##{user['user_id']} | #{user['email']}".ljust(80, '-')

    token = user['tokens'][0]
    if token.nil?
      puts "[WARN] NO TOKEN FOR USER. skipping migration"
      return
    end

    projects = get_projects(token)
    puts "  Found #{projects.count} projects"
    jj projects if @options[:verbose]

    projects.each do |project|
      @migrator.move_queues(project['id'], @options)
    end

  end


  def list_users(options = {})
    per_page = 100
    users = []
    loop do
      res = JSON.parse get('users', {'per_page' => per_page}.merge(options), @token)
      break if res['users'].nil?

      users += res['users']
      if res['users'].count == per_page
        options['previous'] = res['users'][-1]['user_id']
      else
        break
      end
    end
    users
  end

  def get_user(user_id)
    res = JSON.parse(get("users/#{CGI::escape user_id}", {}, @token))
    res['user']
  end

  def get_projects(token)
    res = JSON.parse(get('projects', {}, token))
    res['projects'] || []
  end

  def expand_url(url)
    host, port = @host.split(':')
    port ||= 80
    scheme = port == 443 ? 'https' : 'http'
    "#{scheme}://#{host}:#{port}/1/#{url}"
  end

  def get(url, params, token = nil)
    options = {params: params}.merge(content_type: 'application/json')
    options[:Authorization] = "OAuth #{token}" if token

    RestClient.get(expand_url(url), options)
  end



end