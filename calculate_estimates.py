import sys
import os
import json
import math

# Add the directory containing scoring_utils.py to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scoring_utils import estimate_time_and_velocity

def run_estimates():
    print("Estimated Time (in weeks) for various Competition Scores (C):")
    print(f"Assumptions: P (CPC) = 5.0, Vol (Search Volume) = 1000, A (Authority) = 0.2")
    print("-" * 70)
    print(f"{'Competition (C)':<15} {'Estimated Time (T) in Weeks':<30} {'Estimated Velocity (V)':<20}")
    print("-" * 70)

    # Iterate C from 0.1 to 1.0
    for i in range(1, 11):
        C = i / 10.0
        P = 5.0
        Vol = 1000
        A = 0.2 # Assuming a new site with some initial authority building

        T, V = estimate_time_and_velocity(C=C, P=P, Vol=Vol, A=A)
        print(f"{C:<15.1f} {T:<30.2f} {V:<20}")
    print("-" * 70)

if __name__ == "__main__":
    run_estimates()
