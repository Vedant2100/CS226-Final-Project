# CS226-Final-Project
This project implements a data pipeline for ingesting, transforming, and analyzing vegetation data, with a focus on efficient processing and detection workflows.

## Project Title
Scalable Analysis of Vegetation Anomalies Preceding Plant Disease Outbreaks

## Group Name
CS226 Final Project Group

## Group Number
4

## Members
- **THRISHA AMBAREESHARAJE URS URS** | 862638215 | University of California, Riverside, USA
- **SOHUM DAMANI** | 862621529 | University of California, Riverside, USA
- **YASHASWINI DIGGAVI** | 862620058 | University of California, Riverside, USA
- **VEDANT BORKUTE** | 862552981 | University of California, Riverside, USA
- **SHREYANGSHU BERA** | 862485337 | University of California, Riverside, USA

## Authorship Contribution
This section specifies the responsibilities and work performed by each team member for the project deliverables.

• Thrisha Ambareesharaje Urs Urs: I initiated the data acquisition phase of the project by identifying and defining the study area, including determining the bounding box coordinates for Boulder County. I was responsible for collecting and preparing the satellite datasets using Google Earth Engine, initially experimenting with higher resolutions (10m and 30m). Due to export constraints ( 50 MB per task), I adapted the approach by optimizing to a 52m resolution, enabling efficient data extraction and transfer to AWS S3 while preserving analytical utility. I also contributed to acquiring and processing the ESA WorldCover dataset for the Boulder County region, which was later used for forest masking and anomaly detection. Additionally, I was responsible for implementing the project report in the ACM format and ensured clarity, coherence, and consistency across all sections of the final document.

• Sohum Damani: I established the technical project foundation through my design work on the AWS environment and my development of a data pipeline that processed the 20m Sentinel dataset. My research work involved creating Cloud Optimized GeoTIFF (COG) image format conversion methods which improved data accessibility and developed a PostgreSQL indexing method that enabled faster query execution through its advanced indexing capabilities. I worked together with the team to assess model performance in three different operational modes while I helped the team to design the final project presentation which effectively communicated all technical project milestones.

• Yashaswini Diggavi: I designed and implemented the complete Flask-based web dashboard for real-time vegetation anomaly visualization, including fetching and processing results directly from AWS S3 across three spatial resolutions (20m, 30m, 50m). I built the interactive frontend including monthly trend charts, Z-score histograms, a Leaflet-based geographic anomaly map. In addition, I contributed to the final project report by reviewing and updating multiple sections and adding result figures including scatter plots, monthly trend charts, and Z-score distribution plots.

• Vedant Borkute: I contributed to the initial research on dataset sources and experimented with timelapse generation. I helped establish the project repositories and reports and report outlines, and implementing the data transformation pipeline, the evaluation framework, and the generation of output products such as the timelapses, the analytic tables and the anomaly detection run metadata. I also proposed the study area for the pipeline and coordinated the integration of results and documentation across the team.

• Shreyangshu Bera: I contributed in designing and implementing the Z-score-based anomaly detection logic, including the statistical normalization of pixel-level vegetation indices against historical baselines and the calibration of the anomaly threshold from −2.0 to −1.5 to improve sensitivity toward early-stage vegetation stress. In addition, I assisted with the data acquisition pipeline by exporting satellite imagery from Google Earth Engine to local storage and subsequently uploading the processed scenes to the AWS S3 server.