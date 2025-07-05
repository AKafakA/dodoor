import random

import pickle
import torch
import torch.nn as nn
from torch.autograd import Variable


class RNN(nn.Module):
    def __init__(self, input_size, hidden_size, output_size, all_categories, n_categories, all_letters, n_letters):
        super(RNN, self).__init__()
        self.hidden_size = hidden_size

        self.all_categories = all_categories
        self.n_categories = n_categories
        self.all_letters = all_letters
        self.n_letters = n_letters

        self.i2h = nn.Linear(n_categories + input_size + hidden_size, hidden_size)
        self.i2o = nn.Linear(n_categories + input_size + hidden_size, output_size)
        self.o2o = nn.Linear(hidden_size + output_size, output_size)
        self.dropout = nn.Dropout(0.1)
        self.softmax = nn.LogSoftmax(dim=1)

    def forward(self, category, input_tensor, hidden):
        input_combined = torch.cat((category, input_tensor, hidden), 1)
        hidden = self.i2h(input_combined)
        output = self.i2o(input_combined)
        output_combined = torch.cat((hidden, output), 1)
        output = self.o2o(output_combined)
        output = self.dropout(output)
        output = self.softmax(output)
        return output, hidden

    def init_hidden(self):
        return Variable(torch.zeros(1, self.hidden_size))

    @staticmethod
    def gen_input_tensor(all_letters, n_letters, line):
        tensor = torch.zeros(len(line), 1, n_letters)
        for li in range(len(line)):
            letter = line[li]
            tensor[li][0][all_letters.find(letter)] = 1
        return tensor

    @staticmethod
    def gen_category_tensor(all_categories, n_categories, category):
        li = all_categories.index(category)
        tensor = torch.zeros(1, n_categories)
        tensor[0][li] = 1
        return tensor

    # Sample from a category and starting letter
    def sample(self, category, start_letter='A'):
        category_tensor = Variable(self.gen_category_tensor(self.all_categories, self.n_categories, category))
        input_tensor = Variable(self.gen_input_tensor(self.all_letters, self.n_letters, start_letter))
        hidden = self.init_hidden()

        output_name = start_letter

        max_length = 20
        for i in range(max_length):
            output, hidden = self.forward(category_tensor, input_tensor[0], hidden)
            topv, topi = output.data.topk(1)
            topi = topi[0][0]

            if topi == self.n_letters - 1:
                break
            else:
                letter = self.all_letters[topi]
                output_name += letter

            input_tensor = Variable(self.gen_input_tensor(self.all_letters, self.n_letters, letter))

        return output_name

    # Get multiple samples from one category and multiple starting letters
    def samples(self, category, start_letters='ABC'):
        for start_letter in start_letters:
            yield self.sample(category, start_letter)


if __name__ == "__main__":
    start_letter_maps = {
        "Chinese": ["Z", "L", "W", "Z", "L", "S", "T", "Y", "H", "K"],
        "Japanese": ["S", "T", "W", "Y", "K", "H"],
        "Korean": ["K", "J", "L", "M", "N", "T", "S", "P"],
        "Vietnamese": ["N", "T", "H", "L", "V", "S", "D"],
        "Italian": ["G", "L", "M", "R", "S", "T", "V"],
        "German": ["H", "K", "M", "P", "R", "S", "T", "Z"],
        "Portuguese": ["ã", "é", "í",  "A", "E", "I"],
        "Greek": ["G", "Α", "Β",  "Ξ", "Ο", "Π", "Ρ", "Σ", "Τ", "Υ", ],
        "French": ["J", "M", "P", "R", "S", "T"],
        "English": ["J", "K", "L", "M", "N", "P", "R", "S", "T"],
    }

    parameter_path = "deploy/resources/data/workload_data/rnn/params.pkl"
    with open(parameter_path, 'rb') as pkl:
        params = pickle.load(pkl)

    for language, start_letters in start_letter_maps.items():
        for start_letter in start_letters:
            all_categories = params['all_categories']
            n_categories = params['n_categories']
            all_letters = params['all_letters']
            n_letters = params['n_letters']

            # Check if models are available
            # Download model from S3 if model is not already present
            model_path = "deploy/resources/data/workload_data/rnn/model.pth"

            rnn_model = RNN(n_letters, 128, n_letters, all_categories, n_categories, all_letters, n_letters)
            rnn_model.load_state_dict(torch.load(model_path))
            rnn_model.eval()
            rnn_model.sample(language, start_letter)