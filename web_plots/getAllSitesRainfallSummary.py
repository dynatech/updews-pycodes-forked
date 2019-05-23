from analysis.rainfall import rainfall as rf

def main():
    summary = rf.main(Print=False, write_to_db=False)
    return summary

if __name__ == "__main__":
    summary = main()
    print summary