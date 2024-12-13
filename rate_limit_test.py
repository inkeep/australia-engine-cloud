from pyrate_limiter import Limiter, Duration, Rate, InMemoryBucket

RATE_LIMIT_THRESHOLD = 0.75
ZENDESK_LIMITER = Limiter(
    InMemoryBucket([Rate(400 * RATE_LIMIT_THRESHOLD, Duration.MINUTE)]), max_delay=6000
)


for i in range(500):
    print(i)
    ZENDESK_LIMITER.try_acquire("test")
