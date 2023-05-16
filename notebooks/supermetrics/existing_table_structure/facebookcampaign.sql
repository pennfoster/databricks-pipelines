SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T018_FacebookCampaign](
        [C018_QueryName] [varchar](50) NULL,
        [C018_Year] [varchar](4) NULL,
        [C018_YearMonth] [varchar](7) NULL,
        [C018_Month] [varchar](2) NULL,
        [C018_DayofMonth] [varchar](2) NULL,
        [C018_DayofWeek] [varchar](12) NULL,
        [C018_Account] [varchar](255) NULL,
        [C018_AccountID] [varchar](255) NULL,
        [C018_Date] [date] NULL,
        [C018_CampaignName] [varchar](255) NULL,
        [C018_CampaignID] [varchar](255) NULL,
        [C018_CampaignStatus] [varchar](255) NULL,
        [C018_CampaignDailyBudget] [float] NULL,
        [C018_CampaignStartDate] [varchar](255) NULL,
        [C018_CampaignEndDate] [varchar](255) NULL,
        [C018_Cost] [float] NULL,
        [C018_Reach] [float] NULL,
        [C018_Impressions] [float] NULL,
        [C018_CPMCostPer1000Impressions] [float] NULL,
        [C018_LinkClicks] [float] NULL,
        [C018_UniqueLinkClicks] [float] NULL,
        [C018_CTRLinkClickThroughRate] [varchar](255) NULL,
        [C018_CPCCostPerLinkClick] [float] NULL,
        [C018_CostPerUniqueLinkClick] [float] NULL,
        [C018_ThreeSecondVideoViews] [float] NULL,
        [C018_UniqueThreeSecondVideoViews] [float] NULL,
        [C018_ThirtysecondVideoViews] [float] NULL,
        [C018_VideoWatchesAt25Percent] [float] NULL,
        [C018_VideoWatchesAt50Percent] [float] NULL,
        [C018_VideoWatchesAt75Percent] [float] NULL,
        [C018_VideoWatchesAt95Percent] [float] NULL,
        [C018_VideoWatchesAt100Percent] [float] NULL,
        [C018_VideoAverageWatchTime] [float] NULL,
        [C018_ClicksToPlayVideo] [float] NULL,
        [C018_UniqueClicksToPlayVideo] [float] NULL,
        [C018_AvgCanvasViewTimeSeconds] [float] NULL,
        [C018_AvgCanvasViewPercentage] [varchar](255) NULL,
        [C018_CostPerThreeSecondVideoView] [float] NULL,
        [C018_WebsiteConversions] [float] NULL,
        [C018_WebsiteConversionRate] [varchar](255) NULL,
        [C018_WebsiteCustomConversions] [float] NULL,
        [C018_WebsiteLeads] [float] NULL,
        [C018_WebsitePurchases] [float] NULL,
        [C018_CostPerWebsiteConversion] [float] NULL,
        [C018_CostPerWebsiteCustomConversions] [float] NULL,
        [C018_CostPerWebsiteLead] [float] NULL,
        [C018_CostPerWebsitePurchase] [float] NULL,
        [C018_VideoPlayActions] [float] NULL,
        [C018_Frequency] [float] NULL,
        [C018_Mkwid] [varchar](255) NULL,
        [C018_Pubcode] [varchar](255) NULL,
        [C018_Adkey] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL,
        [C018_CampaignVerticle] [char](3) NULL
    ) ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T018_FacebookCampaign]
ADD
    CONSTRAINT [DF__T018_Face__C018___3DE82FB7] DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO