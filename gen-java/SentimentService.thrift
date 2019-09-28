service SentimentService {
            bool ping(),
            bool services(1: string file, 2: double load, 3: i32 policy),
            void mapService(1: string file),
            void sortService()
        }
