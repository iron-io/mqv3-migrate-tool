FROM debian

RUN apt-get update && apt-get install -y --no-install-recommends ruby-dev git ca-certificates rubygems
RUN git clone http://github.com/thousandsofthem/mqv3-migrate-tool
WORKDIR mqv3-migrate-tool
RUN gem install bundler && bundle install

CMD ./move-queues --help
