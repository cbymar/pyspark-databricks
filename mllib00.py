import math
import numpy as np
# http://spark.apache.org/docs/latest/ml-guide.html

### Quick aside on entropy/information gain
def entropy(lst):
    entropysum = 0
    for _ in lst:
        entropysum += _ * np.log2(_)
    return -1 * entropysum
lst = [0.25, 0.75]
entropy(lst)  # this is our baseline entropy, which is being sent to zero.  So that's the info gain.
entropy([0.75])
entropy([0.25])
# algebraically, we an express the information gain as the prob-weighted sum of the log2 of the
# two probabilities.  It's a weighted sum of the uncertainties that are disappearing.
# Cross-entropy is the average bit length of the message that contains the elimination of uncertainty
lst = [0.35, 0.35, 0.1, 0.1, 0.04, 0.04, 0.01, 0.01]
entropy(lst)  # Entropy is higher now.
# if avg bit length of the uncertainty-eliminating message is higher than the baseline entropy,
# that's the cross-entropy loss.
# algebraically, it's the probability[true dist]-weighted sum of the log2(probabilities[predicted dist])

def crossentropy(lst1, lst2):
    crossentropysum = 0
    for a, b in zip(lst1, lst2):
        crossentropysum += a * np.log2(b)
    return -1 * crossentropysum

lst2 = list(reversed(lst))
lst2, lst

crossentropy(lst,lst2)  # relative entropy.
crossentropy(lst,lst2) - entropy(lst)  # kl-divergence


