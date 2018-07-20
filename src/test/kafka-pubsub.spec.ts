import { KafkaPubSub } from '../index'

const mockWrite = jest.fn((msg) => msg)
const mockProducer = jest.fn(() => ({
  write: mockWrite
}))
const mockConsumer = jest.fn(() => {})
const topics = ['test-topic1', 'test-topic2']
const host = 'localhost:9231'
const pubsub = new KafkaPubSub({
  topics,
  host
})

describe('KafkaPubSub', () => {
  it('should create producer/consumers correctly', () => {
    const onMessage = jest.fn()
    const testChannel = 'testChannel'
    expect(mockProducer).toBeCalledWith(topics)
    expect(mockConsumer).toBeCalledWith(topics)
  })
  it('should subscribe and publish messages correctly', async () => {
    const channel = 'test-channel'
    const onMessage = jest.fn()
    const payload = {
      channel,
      id: 'test',
    }
    const subscription = await pubsub.subscribe(channel, onMessage)
    pubsub.publish(channel, payload)
    expect(mockWrite).toBeCalled()
    expect(mockWrite).toBeCalledWith(new Buffer(JSON.stringify(payload)))
  })
})