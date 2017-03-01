require 'test_helper'

class IronHide::Storage::RabbitMQAdapterTest < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::IronHide::Storage::RabbitMQAdapter::VERSION
  end

  def test_it_does_something_useful
  end
end
