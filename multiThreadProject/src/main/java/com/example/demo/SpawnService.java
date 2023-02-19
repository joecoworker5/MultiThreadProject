package com.example.demo;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
public class SpawnService {

    Random r = new Random();
    int randomPageA = r.nextInt(100) + 1;
    int randomPageB = r.nextInt(100) + 1;

    public List<String> siteAPageRandomContent = new ArrayList<String>() {
        {
            for (int i = 0; i <= randomPageA; i++) {
                add("siteA" + i);
            }
        }
    };

    public List<String> siteBPageRandomContent = new ArrayList<String>() {
        {
            for (int i = 0; i <= randomPageB; i++) {
                add("siteB" + i);
            }
        }
    };

    public Integer getPage(Site site) {
        // 爬頁數要花 1s
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            //doNothing
            return 0;
        }
        switch (site) {
            case A:
                return randomPageA;
            default:
                return randomPageB;
        }

    }

    public List<String> getSiteInfo(Site site,
                                    List<Integer> pages) {
        List<String> result = new ArrayList<>();
        for (Integer page : pages) {

            //每爬一個 page 需花 0.5 s
            try {
                Thread.sleep(500);
                switch (site) {
                    case A:
                        result.add(siteAPageRandomContent.get(page));
                        break;
                    case B:
                        result.add(siteBPageRandomContent.get(page));
                }
            } catch (InterruptedException e) {
                //doNothing
            }
        }

        return result;
    }



    public enum Site {
        A("siteA"),
        B("siteB");

        String description;
        Site(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

}
