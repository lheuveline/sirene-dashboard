from Worker import Worker

def main():
    worker = Worker()
    worker.merge_on_siren(coalesce = 8)
    worker.merge_on_postal_codes(coalesce = 8)

if __name__ == "__main__":
    main()