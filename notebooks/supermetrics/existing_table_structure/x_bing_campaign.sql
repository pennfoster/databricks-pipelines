SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T008_BingCampaign](
        [C008_QueryName] [varchar](50) NULL,
        [C008_Date] [date] NULL,
        [C008_Year] [varchar](255) NULL,
        [C008_YearMonth] [varchar](255) NULL,
        [C008_Month] [varchar](255) NULL,
        [C008_DayofMonth] [varchar](255) NULL,
        [C008_DayofWeek] [varchar](255) NULL,
        [C008_CampaignName] [varchar](255) NULL,
        [C008_CampaignID] [varchar](255) NULL,
        [C008_AccountName] [varchar](255) NULL,
        [C008_AccountID] [varchar](255) NULL,
        [C008_AccountNumber] [varchar](255) NULL,
        [C008_STATUS] [varchar](255) NULL,
        [C008_CampaignType] [varchar](255) NULL,
        [C008_CampaignLabels] [varchar](255) NULL,
        [C008_Impressions] [float] NULL,
        [C008_Clicks] [float] NULL,
        [C008_Cost] [float] NULL,
        [C008_CTRPercentage] [float] NULL,
        [C008_CPC] [float] NULL,
        [C008_CPM] [float] NULL,
        [C008_AveragePosition] [float] NULL,
        [C008_Conversions] [float] NULL,
        [C008_ConversionRate] [varchar](255) NULL,
        [C008_ConversionsPerImpressionRate] [varchar](255) NULL,
        [C008_CostPerConversion] [float] NULL,
        [C008_RevenuePerConversion] [float] NULL,
        [C008_Revenue] [float] NULL,
        [C008_ReturnOnAdSpend] [float] NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL,
        [C008_CampaignVerticle] [char](3) NULL
    ) ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T008_BingCampaign]
ADD
    DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO