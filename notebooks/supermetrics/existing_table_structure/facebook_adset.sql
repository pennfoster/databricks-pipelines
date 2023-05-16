SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T017_FacebookAdSet](
        [C017_QueryName] [varchar](50) NULL,
        [C017_Year] [varchar](4) NULL,
        [C017_YearMonth] [varchar](7) NULL,
        [C017_Month] [varchar](2) NULL,
        [C017_DayofMonth] [varchar](2) NULL,
        [C017_DayofWeek] [varchar](12) NULL,
        [C017_Account] [varchar](255) NULL,
        [C017_AccountID] [varchar](255) NULL,
        [C017_Date] [date] NULL,
        [C017_CampaignName] [varchar](255) NULL,
        [C017_CampaignID] [varchar](255) NULL,
        [C017_CampaignStatus] [varchar](255) NULL,
        [C017_CampaignDailyBudget] [float] NULL,
        [C017_CampaignStartDate] [varchar](255) NULL,
        [C017_CampaignEndDate] [varchar](255) NULL,
        [C017_AdSetName] [varchar](255) NULL,
        [C017_AdSetID] [varchar](255) NULL,
        [C017_AdSetStatus] [varchar](255) NULL,
        [C017_AdSetStartTime] [varchar](255) NULL,
        [C017_AdSetEndTime] [varchar](255) NULL,
        [C017_AdSetTargeting] [varchar](max) NULL,
        [C017_Cost] [float] NULL,
        [C017_Reach] [float] NULL,
        [C017_Impressions] [float] NULL,
        [C017_CPMCostPer1000Impressions] [float] NULL,
        [C017_LinkClicks] [float] NULL,
        [C017_UniqueLinkClicks] [float] NULL,
        [C017_CTRLinkClickThroughRate] [varchar](255) NULL,
        [C017_CPCCostPerLinkClick] [float] NULL,
        [C017_CostPerUniqueLinkClick] [float] NULL,
        [C017_ThreeSecondVideoViews] [float] NULL,
        [C017_UniqueThreeSecondVideoViews] [float] NULL,
        [C017_ThirtysecondVideoViews] [float] NULL,
        [C017_VideoWatchesAt25Percent] [float] NULL,
        [C017_VideoWatchesAt50Percent] [float] NULL,
        [C017_VideoWatchesAt75Percent] [float] NULL,
        [C017_VideoWatchesAt95Percent] [float] NULL,
        [C017_VideoWatchesAt100Percent] [float] NULL,
        [C017_VideoAverageWatchTime] [float] NULL,
        [C017_ClicksToPlayVideo] [float] NULL,
        [C017_UniqueClicksToPlayVideo] [float] NULL,
        [C017_AvgCanvasViewTimeSeconds] [float] NULL,
        [C017_AvgCanvasViewPercentage] [varchar](255) NULL,
        [C017_CostPerThreeSecondVideoView] [float] NULL,
        [C017_WebsiteConversions] [float] NULL,
        [C017_WebsiteConversionRate] [varchar](255) NULL,
        [C017_WebsiteCustomConversions] [float] NULL,
        [C017_WebsiteLeads] [float] NULL,
        [C017_WebsitePurchases] [float] NULL,
        [C017_CostPerWebsiteConversion] [float] NULL,
        [C017_CostPerWebsiteCustomConversions] [float] NULL,
        [C017_CostPerWebsiteLead] [float] NULL,
        [C017_CostPerWebsitePurchase] [float] NULL,
        [C017_VideoPlayActions] [float] NULL,
        [C017_Frequency] [float] NULL,
        [C017_Mkwid] [varchar](255) NULL,
        [C017_Pubcode] [varchar](255) NULL,
        [C017_Adkey] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T017_FacebookAdSet]
ADD
    CONSTRAINT [DF__T017_Face__C017___308E3499] DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO