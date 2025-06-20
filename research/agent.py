from tabnanny import verbose
import numpy as np
import random
from tensorflow.keras.layers import Dense, Input
from tensorflow.keras.models import Sequential
from tqdm import tqdm
from tensorflow.keras.optimizers import Adam
import pandas as pd
from collections import deque


NUM_FEATURES = 5

class ActionSpace:
    def sample(self):
        return random.randint(0, 1)

class Finance:
    url = 'https://certificate.tpq.io/rl4finance.csv'
    def __init__(self, symbol, feature,
                 min_accuracy=0.485, n_features=4):
        self.symbol = symbol
        self.feature = feature
        self.n_features = n_features
        self.action_space = ActionSpace()
        self.min_accuracy = min_accuracy
        self._get_data()
        self._prepare_data()
    def _get_data(self):
        self.raw = pd.read_csv(self.url,
                index_col=0, parse_dates=True)


    def _prepare_data(self):
        self.data = pd.DataFrame(self.raw[self.symbol]).dropna()
        self.data['r'] = np.log(self.data / self.data.shift(1))
        self.data['d'] = np.where(self.data['r'] > 0, 1, 0)
        self.data.dropna(inplace=True)
        self.data_ = (self.data - self.data.mean()) / self.data.std()
    def reset(self):
        self.bar = self.n_features
        self.treward = 0
        state = self.data_[self.feature].iloc[
            self.bar - self.n_features:self.bar].values
        return state, {}
    
    def step(self, action):
        if action == self.data['d'].iloc[self.bar]:
            correct = True
        else:
            correct = False
        reward = 1 if correct else 0
        self.treward += reward
        self.bar += 1
        self.accuracy = self.treward / (self.bar - self.n_features)
        if self.bar >= len(self.data):
            done = True
        elif reward == 1:
            done = False
        elif (self.accuracy < self.min_accuracy) and (self.bar > 15):
            done = True
        else:
            done = False
        next_state = self.data_[self.feature].iloc[
            self.bar - self.n_features:self.bar].values
        return next_state, reward, done, False, {}


fin = Finance(symbol='EUR=', feature='EUR=', n_features=NUM_FEATURES)

print(list(fin.raw.columns))

opt = Adam(learning_rate=0.001)

class Agent():
    def __init__(self, env, num_actions=2, num_features=10):
        """
        Initialise parameters
        """
        self.env = env
        self.gamma = 0.1
        self.epsilon = 0.9
        self.num_features = num_features
        self.min_epsilon = 0.02
        self.num_actions = num_actions
        self.memory = deque(maxlen=1000)
        self.model = self.build()
        self.iterations = 5000
        self.replay_size = 2000
        self.batch_size = 32
        self.decay_factor = 0.01
        self.rewards = []
        self.max_reward = 0
        
    def build(self):
        
        # Model
        model = Sequential()
        model.add(Input(shape=(self.num_features,)))
        model.add(Dense(24, activation="relu"))
        model.add(Dense(24, activation="relu"))
        model.add(Dense(self.num_actions, activation="linear"))
        model.compile(loss="mse", optimizer=opt)
        return model
    
    def act(self, curr_state):
        
        # Exploration
        if self.epsilon > random.random():
            random_action = self.env.action_space.sample()
            return random_action
    
        # Exploitation
        actions_policy = self.model.predict(curr_state, verbose=0)
        action = np.argmax(actions_policy)
        return action
        
    def replay(self):
        
        print("Replay has started")
        self.random_batch = random.sample(self.memory, self.batch_size)
        
        for state, action, next_state, treward, done in self.random_batch:
            if not done:
                treward += self.gamma * np.max(self.model.predict(next_state, verbose=0))
            
            predicted_action_policy = self.model.predict(state, verbose=0)
            predicted_action_policy[0, action] = treward
            self.model.fit(state, predicted_action_policy, epochs=5)
            
        if self.epsilon > self.min_epsilon:
            self.epsilon *= (1 - self.decay_factor)
            
            
    def learn(self, num_episodes=20):
        """
        """
        for e in tqdm(range(num_episodes)):
            print(f'{e} episode has started')
            state, _ = self.env.reset()
            state = np.reshape(state, [1, self.num_features])
            for _ in range(self.iterations):
                action = self.act(state)
                next_state, treward, done, _, _ = self.env.step(action)
                
                next_state = np.reshape(next_state, [1, self.num_features])
                self.memory.append((state, action, next_state, treward, done))
                if done:
                    self.rewards.append(treward)
                    self.max_reward = max(self.max_reward, treward)
                    log = f'episode: {e} | treward: {treward} | max_reward: {self.max_reward}'
                    print(log)
                    break
                
                
                state = next_state
                
                
            if len(self.memory) >= self.replay_size:
                    
                    self.replay()
                    print("Replay has finished")
                    self.memory = []
                    
            print(f'{e} episode has completed')
            

    def test(self, num_episodes=5):
        for ep in range(num_episodes):
            state, _ = self.env.reset()
            state = np.reshape(state, [1, self.num_features])
            total_reward = 0
            done = False
            while not done:
                action = np.argmax(self.model.predict(state, verbose=0)[0])
                next_state, reward, done, _ = self.env.step(action)
                state = np.reshape(next_state, [1, self.num_features])
                total_reward += reward
            print(f"Test Episode {ep+1} - Total Reward: {total_reward}")



agent = Agent(env=fin, num_actions=2, num_features=NUM_FEATURES)

agent.learn(num_episodes=100)