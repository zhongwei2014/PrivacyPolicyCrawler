package crawler;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import crawler.elastic.ElasticClient;
import crawler.elastic.ElasticContent;
import crawler.flag.FlagParser;
import mybatis.DomainFlag;
import mybatis.Flag;
import mybatis.Domain;
import mybatis.dao.CrawlerDataDaoFactory;
import mybatis.dao.MyBatisConnectionFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSessionFactory;
import org.jsoup.internal.StringUtil;


import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResultProcessor {
    public static void main(String[] args) throws IOException {
        ElasticClient elasticClient = new ElasticClient();
        elasticClient.init();



        String dir = "/Users/weizhong/Documents/workspace/stormcrawler/data/htmlfiles1/";

//        String input = "/Users/weizhong/Documents/workspace/stormcrawler/LR_seeds.txt";

        String input = "/Users/weizhong/Documents/workspace/stormcrawler/sharethis_uniq.txt";
        BufferedReader reader = new BufferedReader(new FileReader(input));
//        BufferedWriter writer = new BufferedWriter(new FileWriter(input + ".content.0203"));
        BufferedWriter writer1 = new BufferedWriter(new FileWriter(input + ".urls"));

        PrintWriter flagWriter = new  PrintWriter(new File(input+".domain_flag.txt.0303"));

        final SqlSessionFactory sessions
                = MyBatisConnectionFactory.newSqlSessionFactory(new File("src/main/resources"));
        final CrawlerDataDaoFactory daoFactory = new CrawlerDataDaoFactory(sessions);

//        Map<String, Rule> rules =
        FlagParser.parseRules("src/main/resources/flags/", daoFactory);
//        Map<String, Domain> map = new HashMap<>();

        List<Flag> flags = daoFactory.flagMapper().selectAll();


        Map<String, Set<String>> domainFlagMap = new HashMap<String, Set<String>>();

        String line;
        while((line = reader.readLine()) != null) {
            String host = line.toLowerCase(Locale.ROOT).replaceAll("https://", "")
            .replaceAll("http://", "")
                    .replaceAll("/.*", "")
                    .replaceAll("www.", "");

            Set<String> domainFlags = domainFlagMap.get(host);
            if(domainFlags == null)
                domainFlags = new HashSet<>();

            ElasticContent result = elasticClient.domainContent(host);
//            ElasticContent   result1 = FileResult.getContent(host);

            List<List<String>> rows;
            rows = result.getRows();

            List<String> outLinks = new ArrayList<>();
            for(List<String> row: rows) {
                outLinks.addAll(getOutLinks(row));
            }

            for(String outLink: outLinks) {
                List<String> linkResult = elasticClient.urlContent(outLink);
                if(linkResult!=null && !linkResult.isEmpty()) {
                    System.out.println("from link: " + outLink + "\t" + linkResult.size());
                    rows.add(linkResult);
                }
            }


//            System.out.println(host + "      " + result.getRows().size() + "\t" + rows.size());

            if(rows.size() < 2) {
                writer1.write("http://" + host);
                writer1.newLine();
            }
            if(rows.size() == 0) {
//                writer1.write(host);
//                writer1.newLine();
            } else {
                domainFlags.add("home_page_captured");
            }
            for(List<String> row: rows) {
                String domainName = row.get(0);

                String url = row.get(1);

                String content = row.get(2);

                String outlinks = row.get(3);
                File domainDir = new File(dir + domainName);
                if(!domainDir.exists()) {
                    domainDir.mkdir();
                }
                String fileName = "url" + String.valueOf(url.hashCode());
                File file = new File(domainDir + "/" + fileName);
                if(!file.exists()) {
                    PrintWriter writer2 = new PrintWriter(new File(domainDir + "/" + fileName));

                    writer2.println(domainName);
                    writer2.println(url);
                    writer2.println(content);
                    writer2.close();
                }

                Domain domain = daoFactory.domainMapper().selectByName(domainName);
                if(domain == null) {
                    daoFactory.domainMapper().insert(new Domain(domainName));
                    daoFactory.commit();
                }

                domain = daoFactory.domainMapper().selectByName(domainName);

                for(Flag flag: flags) {
                  DomainFlag  domainFlag = checkRule(flag, url, content, domain);
//                  DomainFlag old = daoFactory.domainFlagMapper().selectByDomainAndFlag(domain.getId(), flag.getId());

                    if(domainFlag.getValue() != 0) {
                        domainFlags.add(flag.getName());
                    }

//                  if(domainFlag.getValue() != 0  && old == null) {
//                      daoFactory.domainFlagMapper().insert(domainFlag);
//                      daoFactory.commit();
//                  }
                }



            }

            domainFlagMap.put(host, domainFlags);
        }

        List<String> keys = Lists.newArrayList(domainFlagMap.keySet());
        Collections.sort(keys);
        for(String d: keys) {
            Set<String> x = domainFlagMap.get(d);
//            if(!x.isEmpty()) x.add("home_page_captured");
            flagWriter.println(d + "\t" + x);
        }


        daoFactory.getSession().close();
        reader.close();
        elasticClient.close();
//        writer.close();
        writer1.close();
        flagWriter.close();
    }




    private static DomainFlag checkRule(
                                        Flag flag,
                                        String url,
                                        String content,
                                        Domain domain) {
        Pattern p = Pattern.compile(flag.getPattern());
        Matcher m;
        if(flag.getType().equals("footer")) {

                m = p.matcher(content);

        } else {
            m = p.matcher(content.toLowerCase());
        }

        DomainFlag domainFlag = new DomainFlag();

        if(m.find()) {

            String x = m.group(0);

            //System.out.println(domain.getUrl() + "\t" + extractSentence(content, start, end));

            domainFlag.setDomainId(domain.getId());
            domainFlag.setFlagId(flag.getId());
            domainFlag.setTerm(x);
            int start = m.start();
            int end = m.end();
            domainFlag.setText(extractSentence(content, start, end));
            domainFlag.setValue(1f);
            if(url.length()>=300)
                url = url.substring(0, 299);
            domainFlag.setUrl(url);
            domainFlag.setUpdatedAt(System.currentTimeMillis());
        } else {

            domainFlag.setValue(0f);
        }
        return domainFlag;
    }

    private static String extractSentence(String content, int start, int end) {

        int first = content.lastIndexOf('.', start);
        int last = content.indexOf('.', end);


        if(first < 0 && last > 0)
            return content.substring(0, Math.min(999,last));
        else if(last <0)
            return content.substring(first+1, Math.min(first+1+999, content.length()));
        else
            return content.substring(first+1, Math.min(first+1+999, last));
    }

    private static List<String> getOutLinks(List<String> row) {
        List<String> links = new ArrayList<String>();

        String fromRow = row.get(3);

        if(StringUtils.isNoneBlank(fromRow)) {
            List<String> lines = Splitter.on('\n').omitEmptyStrings().trimResults()
                    .splitToList(fromRow);
            for(String line: lines) {
                links.add(line.substring(line.indexOf("\t")).trim());
            }
        }


        return links;
    }
}
