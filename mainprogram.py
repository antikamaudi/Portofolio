import pandas as pd
import requests as rq
from io import BytesIO
import streamlit as st
import data_and_attributes as da
import visualization as vis

#First configuration of streamlit
st.set_page_config(layout = "wide")

def getDataClean() -> pd.DataFrame:
    url      = 'https://github.com/antikamaudi/portofolio/blob/main/netflixdatarev.xlsx?raw=true'
    data     = pd.read_excel(BytesIO(rq.get(url).content), sheet_name = 'dim_netflix')
    datatype = {'show_id' : 'string',
                'video_type' : 'string',
                'title' : 'string',
                'director' : 'string',
                'cast' : 'string',
                'country' : 'string',
                'date_added' : 'datetime',
                'release_year' : 'int',
                'duration' : 'string',
                'listed_in' : 'string',
                'description' : 'string',
                'rating' : 'string',
                'avg_score' : 'float',
                'viewers_number' : 'float',
                'viewed_number' : 'float',
                'avg_view_completion_pct' : 'float'}
    
    data = da.adjustmentDataType(data, mappingCol = datatype)
    
    replacecountry = {'United States'  : 'United States of America',
                      'Russia'         : 'Russian Federation',
                      'Soviet Union'   : 'Russian Federation',
                      'Vietnam'        : 'Viet Nam',
                      'Czech Republic' : 'Czechia',
                      'West Germany'   : 'Germany'}
    
    data['country'] = data['country'].str.replace('\,.*', '', regex = True)
    data['country'] = data['country'].str.strip()
    data['country'] = data['country'].fillna('Unknown')
        
    for rep, repwith in replacecountry.items():
        data['country'] = data['country'].str.replace(rep, repwith)
    
    data['year_added'] = data['date_added'].dt.year
    
    datacountrycode = da.getCountryCode()
    datacountrycode = datacountrycode[['country', 'countrycode']]
    data = data.merge(datacountrycode, on = ['country'], how = 'left')
    return data, datacountrycode

def textMarkdown(text : str, **parameter):
    align = parameter.get('align', 'left')
    color = parameter.get('color', 'black')
    text  = "<p style='text-align: {}; color: {};'>{}</p>".format(align, color, text)
    return text
    
def countOfMovieProductionPerCountry(data : pd.DataFrame, datacountrycode : pd.DataFrame, showtype : str) -> pd.DataFrame :
    if(showtype == 'All'):
        countofmovie = data.groupby(['country', 'countrycode', 'video_type']).agg(countofmoviepercountry = ('show_id', pd.Series.nunique))
        countofmovie = countofmovie.reset_index()
        countofmovie = countofmovie.merge(datacountrycode, on = ['country', 'countrycode'], how = 'right')
        countofmovie = countofmovie.pivot(index = ['country', 'countrycode'], columns = 'video_type', values = 'countofmoviepercountry')
        countofmovie = countofmovie.fillna(value = {'Movie' : 0, 'TV Show' : 0})
        countofmovie['Total'] = countofmovie['Movie'] + countofmovie['TV Show']
        countofmovie = countofmovie.reset_index()
        countofmovie[['Movie', 'TV Show', 'Total']] = countofmovie[['Movie', 'TV Show', 'Total']].astype('int')
    else:
        countofmovie = data.groupby(['country', 'countrycode']).agg(countofmoviepercountry = ('show_id', pd.Series.nunique))
        countofmovie = countofmovie.reset_index()
        countofmovie = countofmovie.merge(datacountrycode, on = ['country', 'countrycode'], how = 'right')
        countofmovie = countofmovie.fillna(value = {'countofmoviepercountry' : 0})
        countofmovie['countofmoviepercountry'] = countofmovie['countofmoviepercountry'].astype('int')
    return countofmovie

def compareShowType(data : pd.DataFrame) -> pd.DataFrame:
    countshowtype = data.groupby(['country', 'video_type']).agg(countshowtype = ('show_id', pd.Series.count))
    countshowtype = countshowtype.reset_index()
    return countshowtype

def countShowPerYear(data : pd.DataFrame) -> pd.DataFrame:
    countshowyearly = data.groupby(['year_added']).agg(countshowperyear = ('show_id', pd.Series.nunique))
    countshowyearly = countshowyearly.reset_index()
    return countshowyearly
    
def compareShowTypePerYear(data : pd.DataFrame, years : list, showtype : str) -> pd.DataFrame:
    datayears   = pd.DataFrame({'year_added' : years})
    datacountry = pd.DataFrame({'country'    : data['country'].unique().tolist()})
    
    if(showtype == 'All'):
        data['video_type'] = 'total'
    datashow    = pd.DataFrame({'video_type' : data['video_type'].unique().tolist()})
    datayears['key'], datacountry['key'], datashow['key'] = 0, 0, 0
    datacountry = datacountry.merge(datayears, on = 'key', how = 'outer')\
                             .merge(datashow, on = 'key', how = 'outer')\
                             .drop(columns = ['key'])
    
    countshowtypeyearly = data.groupby(['country', 'video_type', 'year_added']).agg(countshowtype = ('show_id', pd.Series.count))
    countshowtypeyearly = countshowtypeyearly.reset_index()
    countshowtypeyearly = countshowtypeyearly.merge(datacountry, on = ['year_added', 'country', 'video_type'], how = 'right')
    countshowtypeyearly = countshowtypeyearly.fillna(value = {'countshowtype' : 0})
    countshowtypeyearly = countshowtypeyearly.dropna(subset = ['year_added'])
    countshowtypeyearly[['year_added', 'countshowtype']] = countshowtypeyearly[['year_added', 'countshowtype']].astype('int')
    return countshowtypeyearly

def compareRating(data : pd.DataFrame) -> pd.DataFrame:
    datarating = pd.DataFrame({'rating'     : data['rating'].unique().tolist()})
    datashow   = pd.DataFrame({'video_type' : data['video_type'].unique().tolist()})
    datarating['key'], datashow['key'] = 0, 0
    datarating = datarating.merge(datashow, on = 'key', how = 'outer')\
                             .drop(columns = ['key'])
                         
    countviewed = data.groupby(['rating', 'video_type']).agg(countviewersperrating = ('viewed_number', pd.Series.sum))
    countviewed = countviewed.reset_index()
    countviewed['countviewersperrating'] = countviewed['countviewersperrating'].astype('int')
    countviewed = datarating.merge(countviewed, on = ['rating', 'video_type'], how = 'left')
    countviewed = countviewed.fillna(value = {'countviewersperrating' : 0})
    countviewed = countviewed.pivot(index = ['rating'], columns = 'video_type', values = 'countviewersperrating')
    countviewed = countviewed.reset_index()
    countviewed['rating'] = countviewed['rating'].str.replace('nan', 'Unknown') 
    return countviewed

def countViewersPerRating(data : pd.DataFrame, rating : list, showtype : str) -> pd.DataFrame:
    datarating  = pd.DataFrame({'rating'  : rating})
    datarating['rating'] = datarating['rating'].str.replace('nan', 'Unknown')
    datacountry = pd.DataFrame({'country' : data['country'].unique().tolist()})
        
    datarating['key'], datacountry['key'] = 0, 0
    datacountry = datacountry.merge(datarating, on = 'key', how = 'outer')\
                             .drop(columns = ['key'])
    
    sumviewers = data.groupby(['rating', 'country']).agg(sumviewers = ('viewers_number', pd.Series.sum))
    sumviewers = sumviewers.reset_index()
    sumviewers = sumviewers.merge(datacountry, on = ['rating', 'country'], how = 'right')
    sumviewers = sumviewers.fillna(0)
    sumviewers = sumviewers.pivot(index = ['country'], columns = 'rating', values = 'sumviewers')
    return sumviewers

#Define the Dataset
data, datacountrycode = getDataClean()

#Sidebar to show attribute (filter)
LimitCountry = 5
attTypes     = da.getAttribute(data, colAtt = 'video_type', addAll = True) 
attCountry   = da.getAttribute(data, colAtt = 'country')
attYear      = da.getAttribute(data, colAtt = 'year_added')
attRating    = da.getAttribute(data, colAtt = 'rating')

st.sidebar.markdown("**Data Selection to Analyze:** 👇")

attributeType    = st.sidebar.selectbox('Type of the Shows', attTypes)
attributeCountry = st.sidebar.multiselect('Countries (Max {})'.format(LimitCountry), options = attCountry, default = attCountry[0:5])
if len(attributeCountry) > LimitCountry:
    st.warning("You have to select only {} country".format(LimitCountry))
    st.stop()
    
#Proses filter data
datafilter = data
if(attributeType != 'All'):
    datafilter = data.loc[data['video_type'] == attributeType]

datafilter = datafilter.loc[data['country'].isin(attributeCountry)]
datafilter = datafilter.reset_index(drop = True)
       
#Layout 1 : Title 
row0_spacer1, row0_1, row0_spacer2 = st.columns((.1, 7.2, .1))
with row0_1:
    st.title('Comparison of Available Netflix Content with Preferred content by Subscribers')
    st.subheader('Streamlit App by [Antika Maudi Lanthasari](https://dqlab.id)')
    teks1 = 'There is a fierce between video streaming platforms globally, due to the rapid growth\
             in the number of these platforms. Netflix is one of the biggest streaming companies in terms of revenue.\
             But the growth of Netflix subscribers is slower than other video streaming platfrom.\
             Allegedly due to netflix subscription fee is more expensive than others. Although\
             subscription fee is expensive, factor that affect to improve this situation is maintain\
             the subscribers satisfication fulfilled. One of the way is by content according to the taste of\
             the majority of subscibers provided in greater numbers. Comparison between the availability of content\
             that matched the subscriber preferred content can be known through the calculation of the number of\
             viewers for each content. Based on data obatained for January 2021, a comparison was made with several\
             variables between the number of availability and the number of viewers. The variables are the country that produces\
             content for Netflix, the amount of content added each year and rating.'
    teks1 = textMarkdown(teks1, align = 'justify')
    st.markdown('\n{}'.format(teks1), unsafe_allow_html = True)
    st.markdown('\n***Source: cnbc.com***')

#Layout 2 : Show the Data
legend = '\n1.  show_id\t\t\t: identifier of the netflix show (unique)\
          \n2.  type\t\t\t: type of the show (tv show or movie)\
          \n3.  title\t\t\t: title of the show\
          \n4.  director\t\t\t: director of the show\
          \n5.  cast\t\t\t: main cast of the show\
          \n6.  country\t\t\t: country of the netflix show\
          \n7.  date_added\t\t\t: date the show added on netflix\
          \n8.  release_year\t\t: release year of the show\
          \n9.  duration\t\t\t: duration of the show\
          \n10. listed_in\t\t\t: category in which the show listed in\
          \n11. description\t\t\t: description of the show (synopsis)\
          \n12. rating\t\t\t: rating category of the show\
          \n13. avg_score\t\t\t: average of the netflix show score\
          \n14. viewers_number\t\t: number of viewers of the netflix show\
          \n15. viewed_number\t\t: number of the netflix show being watched\
          \n16. avg_view_completion_pct\t: average view completion of the netflix show\
          \n17. countrycode\t\t\t: List of ISO 3166 country codes'    

row1_spacer1, row1_1, row1_spacer2 = st.columns((.1, 7.2, .1))
with row1_1:
    st.header('Processed Data')
    showdata = data.style.format({'avg_score' : '{:,.2f}',
                                  'viewers_number' : '{:.0f}',
                                  'viewed_number' : '{:.0f}',
                                  'avg_view_completion_pct' : '{:,.2f}',
                                  'year_addedd' : '{:.0f}'})
    st.dataframe(showdata)
    see_data = st.expander('Click this to see the detail data 👉')
    with see_data: 
         st.markdown('**Column Description**\n')
         st.text(legend)

#Layout 3 : Title 
dataMap = data
if(attributeType != 'All'):
    dataMap = data.loc[data['video_type'] == attributeType]
    
countmovie = countOfMovieProductionPerCountry(dataMap, datacountrycode, showtype = attributeType)

if(attributeType == 'All'):
    fig1 = vis.plotWorldMap(countmovie, value = 'Total', typeshow = 'All')
else:
    fig1 = vis.plotWorldMap(countmovie, value = 'countofmoviepercountry', typeshow = attributeType)

row2_spacer1, row2_1, row2_spacer2 = st.columns((.1, 7.2, .1))
with row2_1:
    st.header('Countries of Shows Creator on Netflix')
    teks2 = 'For January 2021, the top 3 countries in the Netflix show creator category\
             are the United States, followed by India then in the third position is United Kingdom.\
             Various choices of the shows in terms of type, genre and rating are roduced by the three\
             countries.'
    teks2 = textMarkdown(teks2, align = 'justify')
    st.markdown(teks2, unsafe_allow_html = True)
    st.plotly_chart(fig1, use_container_width = True)

#Layout 4
countshowtype = compareShowType(datafilter)
fig2          = vis.plotBarStack(countshowtype, x = 'country', y = 'countshowtype')

row3_spacer1, row3_1, row3_spacer2 = st.columns((.1, 7.2, .1))  
with row3_1:
    st.header('Compare Countries that Produce and/or Added The Show on Netflix')                             
    st.plotly_chart(fig2, use_container_width = True)
    teks3 = 'The number of shows produced by each country is very different.\
             Only few countries have significant contributions to become the home production for Netflix shows.'
    teks3 = textMarkdown(teks3, align = 'justify')
    st.markdown(teks3, unsafe_allow_html = True)

#Layout 5
countTotalShowPerYear = countShowPerYear(data)
fig3 = vis.plotLine(countTotalShowPerYear, x = 'year_added', y = 'countshowperyear', 
                    xlabel = 'Year', ylabel = 'Total Show')

row4_spacer1, row4_1, row4_spacer2 = st.columns((.1, 7.2, .1))  
with row4_1:
    st.header('Growth Number of Shows added on Netflix with a Certain Period') 
    st.plotly_chart(fig3, use_container_width = True)
    teks4 = 'The number of the shows added in 2020 is decreasing, due to the first year of pandemic that causes\
             a delay in some of the shows released and made during the year. Aside 2020, the number of shows\
             added to Netflix is increasing. Significant increase occured from 2015 to 2016, line up with an increasing \
             number of Netflix subscribers.'
    teks4 = textMarkdown(teks4, align = 'justify')
    st.markdown(teks4, unsafe_allow_html = True)
        
#Layout 6
showProductionPerYear = compareShowTypePerYear(datafilter, years = attYear, showtype = attributeType)
showProductionPerYear['country - type'] = showProductionPerYear['country'] + ' (' + showProductionPerYear['video_type'] + ')'
fig4 = vis.plotLine(showProductionPerYear, x = 'year_added', y = 'countshowtype',
                    xlabel = 'Year ', ylabel = 'Total Production ', colorby = 'country - type')

row5_spacer1, row5_1, row5_spacer2 = st.columns((.1, 7.2, .1))  
with row5_1:
    st.header('Compare Countries that Produce and/or Added The Show on Netflix Per Year')                             
    st.plotly_chart(fig4, use_container_width = True)
    teks5 = 'Melalui grafik ini dapat dibandingkan fluktuasi jumlah tayangan antar Negara\
             yang ditambahkan ke Netflix setiap tahun. Netflix menambahkan tayangan melalui\
             pembelian lisensi tayangan dalam jangka waktu tertentu maupun memproduksi\
             tayangan sendiri. Keputusan perpanjangan lisensi ditentukan dengan jumlah\
             keuntungan ataupun kerugian yang didapat setelah memperhitungkan cost per-minutes\
             setiap tayangan'
    teks5 = textMarkdown(teks5, align = 'justify')
    st.markdown(teks5, unsafe_allow_html = True)
    
#Layout 6
countrating = compareRating(data)
fig5 = vis.plotPyramids(countrating, x = ['Movie', 'TV Show'], y = 'rating')
row6_spacer1, row6_1, row6_spacer2 = st.columns((.1, 7.2, .1))  
with row6_1:
    st.header('Rating Show Options that Available on Netflix')                             
    st.plotly_chart(fig5, use_container_width = True)
    teks6 = 'Pilihan rating tayangan yang disediakan oleh Netflix termasuk beragam,\
             sehingga menghasilkan “wide range of markets” yang mumpuni. Tayangan Netflix \
             tersedia untuk anak umur 7 tahun di bawah pengawasan orangtua. \
             Meskipun pilihan rating yang beragam, namun untuk rating tertentu\
             memiliki jumlah pilihan tayangan yang tidak banyak terutama untuk tipe tv show,\
             dan tayangan didominasi rating TV-MA.'
    teks6 = textMarkdown(teks6, align = 'justify')
    st.markdown(teks6, unsafe_allow_html = True)
    
#Layout 7
countviewers = countViewersPerRating(datafilter, rating = attRating, showtype = attributeType)
fig6 = vis.plotHeatMap(countviewers, colorby = 'rating')

ratingtext = '\n1.  TV-MA\t : Mature Audience Only\
              \n2.  TV-14\t : Parents Strongly Coutioned\
              \n3.  TV-PG\t : Parental Guided Suggested\
              \n4.  TV-Y7\t : Directed to Older Children\
              \n5.  TV-Y7-FV\t : Directed to Older Children Fantasy Violence\
              \n6.  TV-Y\t : All Children\
              \n7.  TV-G\t : General Audience\
              \n8.  NR\t\t : Not Rated\
              \n9.  R\t\t : Restricted\
              \n10. PG-13\t : Parents Strongly Coutioned (age : 13)\
              \n11. PG\t\t : Parental Guided Suggested\
              \n12. NC-17\t : Clearly Adult\
              \n13. G\t\t : General Audience'

row7_spacer1, row7_1, row7_spacer2 = st.columns((.1, 7.2, .1))  
with row7_1:
    st.header('Compare Rating Show on Netflix')   
    teks7 = 'Rating yang mendominasi pilihan tayangan sejalan dalam dominasi \
             jumlah penonton tayangan tersebut'  
    teks7 = textMarkdown(teks7, align = 'justify')
    st.markdown(teks7, unsafe_allow_html = True) 
    st.text(ratingtext)                       
    st.plotly_chart(fig6, use_container_width = True)
