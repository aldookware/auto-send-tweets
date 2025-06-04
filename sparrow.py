#!/usr/bin/env python
import boto3
import base64
import random
import json
import logging
import time
import os
from typing import Dict
from twython import Twython


class TwitterBot:
    """Unified Twitter bot with configurable KMS encryption support"""
    
    def __init__(self, config_path: str = 'config.json'):
        """Initialize bot with configuration"""
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.twitter_client = None
        self._setup_twitter_client()
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {config_path} not found")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup structured logging"""
        logger = logging.getLogger(__name__)
        logger.setLevel(getattr(logging, self.config['logging']['level']))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            
            if self.config['logging']['format'] == 'json':
                formatter = logging.Formatter(
                    '{"timestamp":"%(asctime)s","level":"%(levelname)s","message":"%(message)s","module":"%(name)s"}'
                )
            else:
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
            
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _decrypt_with_kms(self, ciphertext: str) -> str:
        """Decrypt ciphertext with AWS KMS"""
        try:
            kms = boto3.client('kms')
            self.logger.info("Decrypting credentials with KMS")
            
            plaintext = kms.decrypt(
                CiphertextBlob=base64.b64decode(ciphertext)
            )['Plaintext']
            
            # Fix: decode bytes to string
            return plaintext.decode('utf-8')
            
        except Exception as e:
            self.logger.error(f"KMS decryption failed: {e}")
            raise
    
    def _load_credentials(self) -> Dict[str, str]:
        """Load Twitter API credentials with optional KMS decryption"""
        creds_file = self.config['twitter']['credentials_file']
        
        try:
            with open(creds_file, 'r') as f:
                credentials = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Credentials file {creds_file} not found")
            raise
        
        # Use environment variables as fallback
        cred_mapping = {
            'consumer_key': 'TWITTER_CONSUMER_KEY',
            'consumer_secret': 'TWITTER_CONSUMER_SECRET', 
            'access_token_key': 'TWITTER_ACCESS_TOKEN',
            'access_token_secret': 'TWITTER_ACCESS_TOKEN_SECRET'
        }
        
        result = {}
        use_kms = self.config['twitter']['use_kms']
        
        for key, env_var in cred_mapping.items():
            if key in credentials:
                value = credentials[key]
                result[key] = self._decrypt_with_kms(value) if use_kms else value
            elif env_var in os.environ:
                result[key] = os.environ[env_var]
                self.logger.info(f"Using environment variable for {key}")
            else:
                raise ValueError(f"Missing credential: {key}")
        
        return result
    
    def _setup_twitter_client(self):
        """Initialize Twitter client with credentials"""
        try:
            creds = self._load_credentials()
            self.twitter_client = Twython(
                creds['consumer_key'],
                creds['consumer_secret'],
                creds['access_token_key'],
                creds['access_token_secret']
            )
            self.logger.info("Twitter client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to setup Twitter client: {e}")
            raise
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry logic"""
        retry_config = self.config['retry']
        max_attempts = retry_config['max_attempts']
        backoff_factor = retry_config['backoff_factor']
        max_delay = retry_config['max_delay']
        
        for attempt in range(max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_attempts - 1:
                    self.logger.error(f"Operation failed after {max_attempts} attempts: {e}")
                    raise
                
                delay = min(backoff_factor ** attempt, max_delay)
                self.logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
    
    def send_tweet(self, tweet_text: str) -> bool:
        """Send a tweet with retry logic and validation"""
        if not tweet_text or len(tweet_text) > 280:
            raise ValueError("Tweet text must be 1-280 characters")
        
        try:
            self._retry_with_backoff(
                self.twitter_client.update_status,
                status=tweet_text
            )
            self.logger.info(f"Tweet sent successfully: {tweet_text[:50]}...")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send tweet: {e}")
            return False
    
    def send_random_tweet(self) -> bool:
        """Send a random tweet from configured options"""
        tweets = self.config['bot']['tweets']
        if not tweets:
            self.logger.error("No tweets configured")
            return False
        
        selected_tweet = random.choice(tweets)
        return self.send_tweet(selected_tweet)


def handler(event, context):
    """AWS Lambda handler function"""
    try:
        bot = TwitterBot()
        success = bot.send_random_tweet()
        
        return {
            'statusCode': 200 if success else 500,
            'body': json.dumps({
                'success': success,
                'message': 'Tweet sent successfully' if success else 'Failed to send tweet'
            })
        }
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Handler error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e)
            })
        }


if __name__ == "__main__":
    # For local testing
    bot = TwitterBot()
    bot.send_random_tweet()